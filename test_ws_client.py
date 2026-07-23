# test_ws_client.py
import asyncio
import websockets

async def main():
    try:
        async with websockets.connect("ws://localhost:8765/ws") as ws:
            print("接続成功。5秒間待機してイベントを待ちます...")
            try:
                async for message in asyncio.wait_for(ws.recv(), timeout=5):
                    print("受信:", message)
            except asyncio.TimeoutError:
                print("5秒間、イベントは来ませんでした(接続自体は維持されています)")
    except Exception as e:
        print("接続失敗:", type(e).__name__, e)

asyncio.run(main())