import asyncio
import json
import os
import time

import psycopg
import websockets


WS_URL = (
    "wss://fstream.binance.com/stream"
    "?streams=btcusdt@bookTicker/btcusdt@trade"
)

LOG_INTERVAL = 10
DATABASE_URL = os.environ["DATABASE_URL"]


def connect_database():
    print("Connecting to PostgreSQL...", flush=True)

    connection = psycopg.connect(DATABASE_URL)
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS btc_market_data (
                id BIGSERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                symbol TEXT NOT NULL,
                mid DOUBLE PRECISION NOT NULL,
                spread DOUBLE PRECISION NOT NULL,
                trade_count INTEGER NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                buy_volume DOUBLE PRECISION NOT NULL,
                sell_volume DOUBLE PRECISION NOT NULL
            )
            """
        )

    print("POSTGRESQL CONNECTED", flush=True)
    print("TABLE btc_market_data READY", flush=True)

    return connection


def save_market_data(
    connection,
    mid,
    spread,
    trade_count,
    volume,
    buy_volume,
    sell_volume,
):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO btc_market_data (
                    symbol,
                    mid,
                    spread,
                    trade_count,
                    volume,
                    buy_volume,
                    sell_volume
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "BTCUSDT",
                    mid,
                    spread,
                    trade_count,
                    volume,
                    buy_volume,
                    sell_volume,
                ),
            )

        return connection

    except Exception as error:
        print(f"Database write error: {error}", flush=True)
        print("Reconnecting to PostgreSQL...", flush=True)

        try:
            connection.close()
        except Exception:
            pass

        connection = connect_database()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO btc_market_data (
                    symbol,
                    mid,
                    spread,
                    trade_count,
                    volume,
                    buy_volume,
                    sell_volume
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "BTCUSDT",
                    mid,
                    spread,
                    trade_count,
                    volume,
                    buy_volume,
                    sell_volume,
                ),
            )

        return connection


async def collect():
    print("BINANCE-SUP BTC COLLECTOR STARTED", flush=True)

    database = connect_database()

    while True:
        try:
            print("Connecting to Binance Futures...", flush=True)

            async with websockets.connect(WS_URL) as websocket:
                print("CONNECTED TO BINANCE FUTURES", flush=True)

                last_save_time = time.time()

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
                    if stream.lower().endswith("@bookticker"):
                        latest_bid = float(data["b"])
                        latest_ask = float(data["a"])

                    # Individual trades
                    elif stream.lower().endswith("@trade"):
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
                        current_time - last_save_time >= LOG_INTERVAL
                        and latest_bid is not None
                        and latest_ask is not None
                    ):
                        mid = (latest_bid + latest_ask) / 2
                        spread = latest_ask - latest_bid

                        database = save_market_data(
                            database,
                            mid,
                            spread,
                            trade_count,
                            trade_volume,
                            buy_volume,
                            sell_volume,
                        )

                        print(
                            f"SAVED | "
                            f"mid={mid:.2f} | "
                            f"spread={spread:.2f} | "
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

                        last_save_time = current_time

        except Exception as error:
            print(f"WebSocket error: {error}", flush=True)
            print("Reconnecting in 5 seconds...", flush=True)
            await asyncio.sleep(5)


asyncio.run(collect())
