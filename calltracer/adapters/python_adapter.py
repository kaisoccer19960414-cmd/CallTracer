"""
PythonAdapter
=============

sys.settrace を使って、対象プロジェクト配下のPython関数呼び出し(call/return)を
リアルタイムに取得し、共通フォーマットのイベントとして queue.Queue に書き込む。

設計方針(CallTracer全体設計より):
- 対象コードは一切変更しない(デバッグ接続方式)
- include_paths 配下のファイルのみ対象とし、標準ライブラリ/site-packages は除外
- call と return を call_id で紐付けられるようにしておく(将来の所要時間計算などに流用)
- WebSocket送信やイベント統合はEngine側の責務。Adapterはqueueに書き込むだけ。

既知の制約(MVPでは許容する):
- sys.settrace は「呼び出したスレッド」にしか効かない。threading.settrace を
  併用することで「これから起動するスレッド」には自動適用されるが、
  start() 呼び出し時点で既に起動済みのスレッド(例: アプリ起動時に
  用意される常駐ワーカースレッドプール)には効かない(Python標準の制約)。
  リクエストごとにスレッド/タスクを新規生成する典型的なFastAPI構成では
  問題になりにくいが、既存スレッドプール型のアプリでは一部呼び出しが
  拾えない可能性がある。既存スレッドへの対応(例: threading.setprofile
  との併用や起動済みスレッド一覧の走査)は将来の拡張機能として検討する。
"""

from __future__ import annotations

import itertools
import os
import queue
import sys
import threading
import time
from collections import defaultdict
from types import FrameType
from typing import Any, Callable, Optional


class PythonAdapter:
    """sys.settrace ベースのPython関数呼び出しCollector。

    Engine共通契約:
        start(event_queue) -> 監視を開始し、イベントをevent_queueにputし続ける
        stop() -> 監視を停止する
    """

    def __init__(self, include_paths: list[str]):
        """
        Args:
            include_paths: トレース対象とするディレクトリのプレフィックス一覧。
                           例: ["/home/user/myproject/app"]
                           ここに含まれないファイルの呼び出しは全て無視する。
        """
        # 比較しやすいよう絶対パス・正規化した形で保持しておく
        self._include_paths = [os.path.abspath(p) for p in include_paths]
        self._event_queue: Optional["queue.Queue[dict[str, Any]]"] = None
        self._id_counter = itertools.count(1)
        # スレッドごとに呼び出し深さを管理する(FastAPIの同時リクエストを想定)。
        # キー: threading.get_ident() が返すスレッドID
        self._depth_by_thread: dict[int, int] = defaultdict(int)
        self._prev_trace: Optional[Callable] = None

    # ------------------------------------------------------------------
    # 公開インターフェース(Engineの共通契約)
    # ------------------------------------------------------------------

    def start(self, event_queue: "queue.Queue[dict[str, Any]]") -> None:
        """トレースを開始する。以後、対象コードのcall/returnがqueueに積まれる。"""
        self._event_queue = event_queue
        self._depth_by_thread.clear()
        # 既存のtrace関数があれば退避しておく(stop時に復元するため)
        self._prev_trace = sys.gettrace()
        sys.settrace(self._trace_dispatch)
        # sys.settrace は「呼び出したスレッド」にしか効かない。
        # threading.settrace を併用することで、この後 start() された
        # 新規スレッドにも自動でトレース関数がセットされるようにする。
        # 注意: すでに起動済みの既存スレッドには効かない(Python標準の制約)。
        threading.settrace(self._trace_dispatch)

    def stop(self) -> None:
        """トレースを停止し、必要であれば以前のtrace関数を復元する。"""
        sys.settrace(self._prev_trace)
        threading.settrace(self._prev_trace)
        self._event_queue = None

    # ------------------------------------------------------------------
    # 内部ロジック
    # ------------------------------------------------------------------

    def _is_target(self, filename: str) -> bool:
        """このファイルをトレース対象とするかどうかを判定する。"""
        if not self._include_paths:
            return False
        abs_filename = os.path.abspath(filename)
        return any(
            abs_filename.startswith(prefix) for prefix in self._include_paths
        )

    def _trace_dispatch(self, frame: FrameType, event: str, arg: Any):
        """sys.settrace に渡すコールバック本体。

        'call' イベントの戻り値としてこの関数自身を返すことで、
        その呼び出しに対する 'return' イベントも継続して受け取れる
        (sys.settrace の仕様: ローカルトレース関数を返すとフレーム内で使われる)。
        """
        filename = frame.f_code.co_filename

        if not self._is_target(filename):
            # 対象外のフレームは深追いしない(ローカルトレースを設定しない)
            return None

        if event == "call":
            thread_id = threading.get_ident()
            self._depth_by_thread[thread_id] += 1
            current_depth = self._depth_by_thread[thread_id]

            call_id = f"evt_{next(self._id_counter):05d}"
            self._emit(
                {
                    "id": call_id,
                    "timestamp": time.time(),
                    "source": "python",
                    "type": "call",
                    "depth": current_depth,
                    "thread_id": thread_id,
                    "payload": {
                        "function": frame.f_code.co_name,
                        "file": filename,
                        "line": frame.f_lineno,
                        "args_summary": self._summarize_args(frame),
                    },
                }
            )
            # このフレーム専用のローカルトレース関数を返す(return拾うため)
            frame_call_id = call_id
            frame_depth = current_depth
            frame_thread_id = thread_id

            def local_trace(frame: FrameType, event: str, arg: Any):
                if event == "return":
                    self._emit(
                        {
                            "id": f"{frame_call_id}_ret",
                            "timestamp": time.time(),
                            "source": "python",
                            "type": "return",
                            "depth": frame_depth,
                            "thread_id": frame_thread_id,
                            "payload": {
                                "function": frame.f_code.co_name,
                                "call_id": frame_call_id,
                            },
                        }
                    )
                    # 同じスレッドで depth を1段戻す
                    self._depth_by_thread[frame_thread_id] -= 1
                return local_trace

            return local_trace

        return None

    def _summarize_args(self, frame: FrameType) -> str:
        """引数を軽量な文字列表現に要約する(重い値の全文出力は避ける)。"""
        try:
            arg_names = frame.f_code.co_varnames[: frame.f_code.co_argcount]
            parts = []
            for name in arg_names:
                value = frame.f_locals.get(name, "?")
                text = repr(value)
                if len(text) > 40:
                    text = text[:37] + "..."
                parts.append(f"{name}={text}")
            return ", ".join(parts)
        except Exception:
            # 要約に失敗してもトレース自体は継続させる
            return ""

    def _emit(self, event: dict[str, Any]) -> None:
        if self._event_queue is not None:
            # ノンブロッキング前提。キューが詰まっている場合の扱いはEngine側の設計次第。
            self._event_queue.put(event)