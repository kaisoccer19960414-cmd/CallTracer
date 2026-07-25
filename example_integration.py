"""
既存のFastAPIアプリにCallTracerを組み込む最小サンプル。

前提:
- pip install fastapi uvicorn websockets 済みであること
- 対象アプリのコードは一切変更しない(この統合コードを追加するだけ)

構成:
- 対象アプリ本体: 8000番ポートなど、いつも通り起動
- CallTracer Engine: 8765番ポート(別プロセス)でViewerを配信
  ブラウザで http://localhost:8765/ を開くとタイムラインが見える
- JSAdapter: Chrome DevTools Protocol経由で対象アプリの fetch() を捕捉する

--------------------------------------------------------------------
JS側の動作確認に必要な追加手順(ここが重要です)
--------------------------------------------------------------------
JavaScriptイベントを取得するには、対象アプリを「リモートデバッグポート付きの
Chrome」で開く必要があります。普段使いのChromeとは別に、以下のように
起動してください(既存のChromeプロセスが起動中だと反映されないことがあるので、
専用の user-data-dir を指定するのが確実です)。

Windows (PowerShell) の例:
    & "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" `
        --remote-debugging-port=9222 `
        --user-data-dir="C:\\chrome-debug-profile"

Mac の例:
    open -a "Google Chrome" --args --remote-debugging-port=9222 \\
        --user-data-dir=/tmp/chrome-debug-profile

このChromeで http://localhost:8000/ を開き、表示される
「fetch(user 1)」ボタンをクリックしてください
(アドレスバーへの直接アクセスは「ページ遷移」扱いになり、fetch/XHRとしては
検知されないため、必ずボタン経由でfetch()を発生させる必要があります)。

使い方:
    python example_integration.py
"""

import os
import queue
import threading

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from calltracer.adapters.js_adapter import JSAdapter
from calltracer.adapters.python_adapter import PythonAdapter
from calltracer.engine import Engine

# --- 1. 対象アプリ側(本来はユーザーの既存FastAPIアプリ) -----------------
target_app = FastAPI()


@target_app.get("/", response_class=HTMLResponse)
def demo_page():
    # JS側のイベントを実際に発生させるための最小限のデモページ。
    # アドレスバー直打ちだと"ページ遷移"扱いになりfetchとして検知されないため、
    # ボタン経由でfetch()を呼ぶ形にしている。
    return """
    <html>
    <body style="font-family: sans-serif;">
      <h3>CallTracer 動作確認用ページ</h3>
      <button onclick="callApi()">fetch(user 1)</button>
      <pre id="result"></pre>
      <script>
        async function callApi() {
          const res = await fetch('/api/users/1');
          const data = await res.json();
          document.getElementById('result').textContent = JSON.stringify(data);
        }
      </script>
    </body>
    </html>
    """


@target_app.get("/api/users/{user_id}")
def get_user(user_id: int):
    return fetch_from_db(user_id)


def fetch_from_db(user_id: int):
    return {"id": user_id, "name": "Taro"}


# --- 2. CallTracer側のセットアップ ---------------------------------------
event_queue: "queue.Queue" = queue.Queue()

# 対象アプリのプロジェクトルート配下だけをトレース対象にする
# (os.path.dirnameを使い、OS間のパス区切り文字の違いを吸収する)
project_root = os.path.dirname(os.path.abspath(__file__))
python_adapter = PythonAdapter(include_paths=[project_root])
python_adapter.start(event_queue)

# JSAdapterはChromeの --remote-debugging-port=9222 に接続する。
# Chromeが未起動/未対応の場合は adapter_error イベントがVieweに流れるだけで、
# Python側のトレースには影響しない。
js_adapter = JSAdapter(cdp_http_url="http://localhost:9222")
js_adapter.start(event_queue)

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
    print()
    print("手順:")
    print("  1. リモートデバッグ付きのChromeで http://localhost:8000/ を開く")
    print("     (--remote-debugging-port=9222 で起動したChromeであること)")
    print("  2. 別タブでCallTracer Viewer(http://localhost:8765)を開く")
    print("  3. 対象アプリのページで「fetch(user 1)」ボタンをクリック")
    print("  4. Viewerのタイムラインに JS(fetch_start) → Python(get_user →")
    print("     fetch_from_db) → JS(fetch_end) の流れが表示されるはず")

    t1.join()
    t2.join()