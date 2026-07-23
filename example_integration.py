"""
既存のFastAPIアプリにCallTracerを組み込む最小サンプル。

前提:
- pip install fastapi uvicorn websockets 済みであること
- 対象アプリのコードは一切変更しない(この統合コードを追加するだけ)

構成:
- 対象アプリ本体: 8000番ポートなど、いつも通り起動
- CallTracer Engine: 8765番ポート(別プロセス)でViewerを配信
  ブラウザで http://localhost:8765/ を開くとタイムラインが見える

使い方:
    python example_integration.py
"""

import queue
import threading

import uvicorn
from fastapi import FastAPI

from calltracer.adapters.python_adapter import PythonAdapter
from calltracer.engine import Engine

# --- 1. 対象アプリ側(本来はユーザーの既存FastAPIアプリ) -----------------
target_app = FastAPI()


@target_app.get("/api/users/{user_id}")
def get_user(user_id: int):
    return fetch_from_db(user_id)


def fetch_from_db(user_id: int):
    return {"id": user_id, "name": "Taro"}


# --- 2. CallTracer側のセットアップ ---------------------------------------
event_queue: "queue.Queue" = queue.Queue()

# 対象アプリのプロジェクトルート配下だけをトレース対象にする
adapter = PythonAdapter(include_paths=[__file__.rsplit("/", 1)[0]])
adapter.start(event_queue)

engine = Engine(event_queue)
viewer_app = engine.create_app()


def run_target_app():
    uvicorn.run(target_app, host="0.0.0.0", port=8000, log_level="warning")


def run_viewer_app():
    uvicorn.run(viewer_app, host="0.0.0.0", port=8765, log_level="warning")


if __name__ == "__main__":
    # 対象アプリとViewer用Engineをそれぞれ別スレッドで起動する
    # (MVPではシンプルさ優先。将来は別プロセス分離やCLI化を検討)
    t1 = threading.Thread(target=run_target_app, daemon=True)
    t2 = threading.Thread(target=run_viewer_app, daemon=True)
    t1.start()
    t2.start()

    print("対象アプリ: http://localhost:8000")
    print("CallTracer Viewer: http://localhost:8765")
    print("ブラウザでViewerを開いた状態で、対象アプリの /api/users/1 などにアクセスしてください。")

    t1.join()
    t2.join()