import asyncio
import json
import websockets

WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"


async def collect():
    print("BINANCE-SUP BTC COLLECTOR STARTED", flush=True)

    while True:
        try:
            print("Connecting to Binance...", flush=True)

            async with websockets.connect(WS_URL) as websocket:
                print("CONNECTED TO BINANCE", flush=True)

                async for message in websocket:
                    data = json.loads(message)

                    bid = float(data["b"])
                    ask = float(data["a"])
                    mid = (bid + ask) / 2

                    print(
                        f"BTCUSDT | bid={bid:.2f} | "
                        f"ask={ask:.2f} | mid={mid:.2f}",
                        flush=True,
                    )

        except Exception as error:
            print(f"WebSocket error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


asyncio.run(collect())
