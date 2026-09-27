import asyncio
import json
import time
import websockets

WS_URL = (
    "wss://fstream.binance.com/stream"
    "?streams=btcusdt@bookTicker/btcusdt@aggTrade"
)

LOG_INTERVAL = 10


async def collect():
    print("BINANCE-SUP BTC COLLECTOR STARTED", flush=True)

    while True:
        try:
            print("Connecting to Binance Futures...", flush=True)

            async with websockets.connect(WS_URL) as websocket:
                print("CONNECTED TO BINANCE FUTURES", flush=True)

                last_log_time = 0

                latest_bid = None
                latest_ask = None

                trade_count = 0
                trade_volume = 0.0
                buy_volume = 0.0
                sell_volume = 0.0

                async for message in websocket:
                    message_data = json.loads(message)

                    stream = message_data["stream"]
                    data = message_data["data"]

                    # Best bid / ask
                    if stream.endswith("@bookTicker"):
                        latest_bid = float(data["b"])
                        latest_ask = float(data["a"])

                    # Aggregated trades
                    elif stream.endswith("@aggTrade"):
                        quantity = float(data["q"])

                        trade_count += 1
                        trade_volume += quantity

                        # m=True:
                        # buyer was maker -> aggressive seller
                        if data["m"]:
                            sell_volume += quantity
                        else:
                            buy_volume += quantity

                    current_time = time.time()

                    if (
                        current_time - last_log_time >= LOG_INTERVAL
                        and latest_bid is not None
                        and latest_ask is not None
                    ):
                        mid = (latest_bid + latest_ask) / 2

                        print(
                            f"BTCUSDT | "
                            f"mid={mid:.2f} | "
                            f"trades={trade_count} | "
                            f"volume={trade_volume:.4f} BTC | "
                            f"buy={buy_volume:.4f} | "
                            f"sell={sell_volume:.4f}",
                            flush=True,
                        )

                        trade_count = 0
                        trade_volume = 0.0
                        buy_volume = 0.0
                        sell_volume = 0.0

                        last_log_time = current_time

        except Exception as error:
            print(f"WebSocket error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


asyncio.run(collect())
