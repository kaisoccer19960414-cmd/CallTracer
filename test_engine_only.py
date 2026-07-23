# test_engine_only.py
import queue
import uvicorn

from calltracer.engine import Engine

q = queue.Queue()
engine = Engine(q)
app = engine.create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8765)