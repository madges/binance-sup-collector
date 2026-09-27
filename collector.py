import asyncio
import json
import websockets


WS_URL = (
    "wss://fstream.binance.com/stream"
    "?streams=btcusdt@bookTicker/btcusdt@aggTrade"
)


async def collect():
    print("BINANCE-SUP STREAM DIAGNOSTIC STARTED", flush=True)

    while True:
        try:
            print("Connecting to Binance Futures...", flush=True)

            async with websockets.connect(WS_URL) as websocket:
                print("CONNECTED TO BINANCE FUTURES", flush=True)

                message_count = 0

                async for message in websocket:
                    message_data = json.loads(message)

                    print(
                        "RAW MESSAGE:",
                        message_data,
                        flush=True,
                    )

                    message_count += 1

                    if message_count >= 20:
                        print(
                            "DIAGNOSTIC COMPLETE - waiting...",
                            flush=True,
                        )

                        # Не спамим Railway логами после диагностики
                        while True:
                            await asyncio.sleep(60)

        except Exception as error:
            print(f"WebSocket error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


asyncio.run(collect())
