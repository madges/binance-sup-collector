import asyncio
import json
import websockets


WS_URL = "wss://fstream.binance.com/ws/btcusdt@trade"


async def collect():
    print("BINANCE-SUP TRADE TEST STARTED", flush=True)

    while True:
        try:
            print("Connecting to BTCUSDT trade stream...", flush=True)

            async with websockets.connect(WS_URL) as websocket:
                print("CONNECTED TO TRADE STREAM", flush=True)

                async for message in websocket:
                    data = json.loads(message)

                    print(
                        f"TRADE | "
                        f"price={data['p']} | "
                        f"quantity={data['q']} | "
                        f"maker={data['m']}",
                        flush=True,
                    )

        except Exception as error:
            print(f"WebSocket error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


asyncio.run(collect())
