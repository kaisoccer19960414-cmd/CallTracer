"""
Engine
======

Adapter(PythonAdapter, 将来はJSAdapterなど)が queue.Queue に積んだイベントを
非同期に取り出し、接続中のViewer(WebSocketクライアント)へブロードキャストする。

設計方針(CallTracer全体設計より):
- Adapter側は「どんな形であれ queue.Queue にイベントdictをputするだけ」でよい
- Engineは「取り出して配る」ことだけに責任を持つ
  (イベント統合ロジックは持たず、届いた順=時刻順にそのまま流すだけ)
- FastAPIのWebSocketエンドポイントをViewer用に1本だけ公開する(/ws)
- Adapterは同期コード(queue.Queue)、Engine/Viewerは非同期(asyncio/WebSocket)
  という前提のズレを、専用のブリッジスレッドで吸収する
"""

import asyncio
import json
import queue
import threading
from typing import Any, Optional


class Engine:
    """queue.Queue -> WebSocket のブロードキャストだけを行う薄いオーケストレーター。"""

    def __init__(self, event_queue: "queue.Queue[dict[str, Any]]"):
        self._event_queue = event_queue
        self._clients: set[Any] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._bridge_thread: Optional[threading.Thread] = None
        self._stop_flag = threading.Event()

    # ------------------------------------------------------------------
    # 公開インターフェース
    # ------------------------------------------------------------------

    def create_app(self):
        """Viewer用のWebSocketエンドポイントを持つFastAPIアプリを構築して返す。

        既存アプリに組み込む場合は、このappを別ポートでuvicorn起動するか、
        マウントして使う想定(MVPでは別プロセス/別ポートで動かすのが最もシンプル)。

        fastapiはここで初めてimportする。ブリッジロジック(queue.Queue ->
        非同期ブロードキャスト)自体はWebフレームワークに依存しないため、
        フレームワーク未導入の環境でもEngineの中核ロジックを単体テストできる。
        """
        from fastapi import FastAPI, WebSocket, WebSocketDisconnect
        from fastapi.responses import HTMLResponse
        import os

        app = FastAPI()

        viewer_path = os.path.join(
            os.path.dirname(__file__), "viewer", "timeline.html"
        )

        @app.get("/")
        async def index():
            with open(viewer_path, "r", encoding="utf-8") as f:
                return HTMLResponse(f.read())

        @app.websocket("/ws")
        async def ws_endpoint(websocket: WebSocket):
            await websocket.accept()
            self._clients.add(websocket)
            try:
                while True:
                    # Viewerからのメッセージは使わないが、
                    # 切断(WebSocketDisconnect)を検知するために受信待ちを続ける
                    await websocket.receive_text()
            except WebSocketDisconnect:
                pass
            finally:
                self._clients.discard(websocket)

        @app.on_event("startup")
        async def on_startup():
            self._loop = asyncio.get_event_loop()
            self._start_bridge_thread()

        @app.on_event("shutdown")
        async def on_shutdown():
            self._stop_flag.set()
            if self._bridge_thread is not None:
                self._bridge_thread.join(timeout=1.0)

        return app

    # ------------------------------------------------------------------
    # 内部ロジック
    # ------------------------------------------------------------------

    def _start_bridge_thread(self) -> None:
        """同期queue.Queueをブロッキングgetで監視し続け、
        取れたイベントをasyncioループ側へ安全に受け渡すブリッジスレッド。

        queue.Queueはスレッドセーフだが非同期(await)には対応していないため、
        別スレッドでブロッキングgetし、asyncio.run_coroutine_threadsafeで
        イベントループ側のブロードキャスト処理を起動する形にしている。
        """
        self._stop_flag.clear()

        def worker() -> None:
            while not self._stop_flag.is_set():
                try:
                    event = self._event_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                if self._loop is not None:
                    asyncio.run_coroutine_threadsafe(
                        self._broadcast(event), self._loop
                    )

        self._bridge_thread = threading.Thread(
            target=worker, daemon=True, name="calltracer-bridge"
        )
        self._bridge_thread.start()

    async def _broadcast(self, event: dict[str, Any]) -> None:
        """接続中の全Viewerクライアントへイベントを送信する。

        送信に失敗したクライアントは切断済みとみなしてリストから除外する
        (MVPでは再送やバッファリングは行わない)。
        """
        if not self._clients:
            return
        message = json.dumps(event, ensure_ascii=False)
        dead: list[Any] = []
        for client in self._clients:
            try:
                await client.send_text(message)
            except Exception:
                dead.append(client)
        for client in dead:
            self._clients.discard(client)