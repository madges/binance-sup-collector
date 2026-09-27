import asyncio
import json
import time
import websockets

WS_URL = "wss://fstream.binance.com/ws/btcusdt@bookTicker"
LOG_INTERVAL = 10


async def collect():
    print("BINANCE-SUP BTC COLLECTOR STARTED", flush=True)

    while True:
        try:
            print("Connecting to Binance...", flush=True)

            async with websockets.connect(WS_URL) as websocket:
                print("CONNECTED TO BINANCE", flush=True)

                last_log_time = 0

                async for message in websocket:
                    data = json.loads(message)

                    bid = float(data["b"])
                    ask = float(data["a"])
                    mid = (bid + ask) / 2

                    current_time = time.time()

                    if current_time - last_log_time >= LOG_INTERVAL:
                        print(
                            f"BTCUSDT | bid={bid:.2f} | "
                            f"ask={ask:.2f} | mid={mid:.2f}",
                            flush=True,
                        )
                        last_log_time = current_time

        except Exception as error:
            print(f"WebSocket error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


asyncio.run(collect())
