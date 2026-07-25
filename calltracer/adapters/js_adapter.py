"""
JSAdapter
=========

Chrome DevTools Protocol (CDP) を使って、ブラウザの fetch/XHR 発生を
リアルタイムに取得し、共通フォーマットのイベントとして queue.Queue に書き込む。

設計方針(CallTracer全体設計より):
- ブラウザ側のコードは一切変更しない(Chromeの --remote-debugging-port 機能を使うだけ)
- Network.requestWillBeSent / Network.responseReceived だけを購読する
  (関数呼び出しレベルの詳細が必要ならRuntime/Debuggerドメインが要るが、MVPでは不要)
- requestId を call_id として使い、fetch開始(fetch_start)と終了(fetch_end)を紐付ける
- WebSocket接続には websockets ライブラリを使う(FastAPI/uvicorn利用時に
  既にインストール済みのはずなので、新規依存の追加にはならない)
- Adapter共通契約(start(event_queue) / stop())はPythonAdapterと同じ

前提条件(ユーザー側の準備、これだけは必要):
    対象アプリを開くChromeを、リモートデバッグポート付きで起動しておくこと。
    例:
        chrome.exe --remote-debugging-port=9222

    普段使いのChromeとは別のプロファイルで起動する場合は、
    --user-data-dir=<空のディレクトリ> も併せて指定するとよい。
"""

from __future__ import annotations

import asyncio
import itertools
import json
import queue
import threading
import time
import urllib.request
from typing import Any, Optional


class JSAdapter:
    """Chrome DevTools Protocolベースのfetch/XHR Collector。

    Engine共通契約:
        start(event_queue) -> 監視を開始し、イベントをevent_queueにputし続ける
        stop() -> 監視を停止する
    """

    def __init__(self, cdp_http_url: str = "http://localhost:9222"):
        """
        Args:
            cdp_http_url: Chromeのリモートデバッグ用HTTPエンドポイント。
                          `chrome --remote-debugging-port=9222` した場合は
                          デフォルトの "http://localhost:9222" のままでよい。
        """
        self._cdp_http_url = cdp_http_url.rstrip("/")
        self._event_queue: Optional["queue.Queue[dict[str, Any]]"] = None
        self._id_counter = itertools.count(1)
        self._thread: Optional[threading.Thread] = None
        self._stop_flag: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ------------------------------------------------------------------
    # 公開インターフェース(Engineの共通契約)
    # ------------------------------------------------------------------

    def start(self, event_queue: "queue.Queue[dict[str, Any]]") -> None:
        """CDP接続を別スレッド上のasyncioループで開始する。"""
        self._event_queue = event_queue
        self._thread = threading.Thread(
            target=self._run_in_thread, daemon=True, name="calltracer-js-adapter"
        )
        self._thread.start()

    def stop(self) -> None:
        """監視を停止する。"""
        if self._loop is not None and self._stop_flag is not None:
            self._loop.call_soon_threadsafe(self._stop_flag.set)
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    # ------------------------------------------------------------------
    # 内部ロジック
    # ------------------------------------------------------------------

    def _run_in_thread(self) -> None:
        """このAdapter専用のイベントループをスレッド内に立てて実行する。

        Engine側のasyncioループとは別物であってよい
        (Adapterはqueue.Queueに書き込むだけで、asyncio的な結合は不要なため)。
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._stop_flag = asyncio.Event()
        try:
            loop.run_until_complete(self._main())
        finally:
            loop.close()

    async def _main(self) -> None:
        ws_url = self._discover_websocket_url()
        if ws_url is None:
            self._emit_error(
                "Chromeのリモートデバッグエンドポイントが見つかりません。"
                "chrome --remote-debugging-port=9222 で起動しているか確認してください。"
            )
            return

        import websockets  # 遅延import。JSAdapterを使わない場合は依存不要にする。

        async with websockets.connect(ws_url, max_size=None) as ws:
            await self._send(ws, "Network.enable", {})
            listen_task = asyncio.create_task(self._listen(ws))
            await self._stop_flag.wait()
            listen_task.cancel()

    def _discover_websocket_url(self) -> Optional[str]:
        """Chromeの /json エンドポイントから、最初に見つかったページタブの
        webSocketDebuggerUrl を取得する(標準ライブラリのみで完結させる)。
        """
        try:
            with urllib.request.urlopen(f"{self._cdp_http_url}/json", timeout=3) as resp:
                targets = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

        for target in targets:
            if target.get("type") == "page" and "webSocketDebuggerUrl" in target:
                return target["webSocketDebuggerUrl"]
        return None

    async def _send(self, ws, method: str, params: dict[str, Any]) -> None:
        message = {
            "id": next(self._id_counter),
            "method": method,
            "params": params,
        }
        await ws.send(json.dumps(message))

    async def _listen(self, ws) -> None:
        """CDPからのイベントを受信し続け、fetch/XHR関連だけを拾ってqueueに流す。"""
        async for raw in ws:
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue

            method = message.get("method")
            if method == "Network.requestWillBeSent":
                self._handle_request_will_be_sent(message.get("params", {}))
            elif method == "Network.responseReceived":
                self._handle_response_received(message.get("params", {}))

    def _handle_request_will_be_sent(self, params: dict[str, Any]) -> None:
        resource_type = params.get("type")  # "Fetch" | "XHR" | "Document" | ...
        if resource_type not in ("Fetch", "XHR"):
            return

        request = params.get("request", {})
        request_id = params.get("requestId")
        # wallTimeはUnixエポック秒(壁時計時刻)。Python側のtime.time()と
        # 同じ基準にできるため、こちらを使う(timestampはセッション内の
        # モノトニック時間なので、Python側と直接比較できない)。
        wall_time = params.get("wallTime", time.time())

        self._emit(
            {
                "id": f"js_{request_id}",
                "timestamp": wall_time,
                "source": "javascript",
                "type": "fetch_start",
                "depth": 0,
                "payload": {
                    "url": request.get("url", ""),
                    "method": request.get("method", ""),
                    "call_id": request_id,
                },
            }
        )

    def _handle_response_received(self, params: dict[str, Any]) -> None:
        response = params.get("response", {})
        resource_type = params.get("type")
        if resource_type not in ("Fetch", "XHR"):
            return

        request_id = params.get("requestId")

        self._emit(
            {
                "id": f"js_{request_id}_end",
                "timestamp": time.time(),
                "source": "javascript",
                "type": "fetch_end",
                "depth": 0,
                "payload": {
                    "url": response.get("url", ""),
                    "status": response.get("status"),
                    "call_id": request_id,
                },
            }
        )

    def _emit(self, event: dict[str, Any]) -> None:
        if self._event_queue is not None:
            self._event_queue.put(event)

    def _emit_error(self, message: str) -> None:
        """接続失敗などをViewer側にも見える形で伝える(通常のイベントとは
        typeを分けて "adapter_error" とし、Viewer側は未対応でも無害に流れる)。
        """
        self._emit(
            {
                "id": "js_adapter_error",
                "timestamp": time.time(),
                "source": "javascript",
                "type": "adapter_error",
                "depth": 0,
                "payload": {"message": message},
            }
        )