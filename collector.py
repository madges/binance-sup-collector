import asyncio
import json
import os
import time

import psycopg
import websockets


DATABASE_URL = os.environ["DATABASE_URL"]

WS_URL = (
    "wss://fstream.binance.com/stream"
    "?streams=btcusdt@bookTicker"
    "/btcusdt@aggTrade"
    "/btcusdt@kline_1m"
)

SAVE_INTERVAL = 10


def prepare_database(conn):
    # Таблица 10-секундных рыночных срезов
    conn.execute(
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

    # Таблица 1-минутных свечей
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS btc_klines_1m (
            open_time TIMESTAMPTZ PRIMARY KEY,
            close_time TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            trade_count INTEGER NOT NULL
        )
        """
    )

    conn.commit()


def save_market_snapshot(
    conn,
    mid,
    spread,
    trade_count,
    volume,
    buy_volume,
    sell_volume,
):
    conn.execute(
        """
        INSERT INTO btc_market_data
        (
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
    conn.commit()


def save_kline(conn, kline):
    # Binance присылает обновления свечи постоянно.
    # Сохраняем только окончательно закрытую минутную свечу.
    if not kline["x"]:
        return False

    conn.execute(
        """
        INSERT INTO btc_klines_1m
        (
            open_time,
            close_time,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            trade_count
        )
        VALUES
        (
            to_timestamp(%s / 1000.0),
            to_timestamp(%s / 1000.0),
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (open_time)
        DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            trade_count = EXCLUDED.trade_count
        """,
        (
            kline["t"],
            kline["T"],
            kline["s"],
            float(kline["o"]),
            float(kline["h"]),
            float(kline["l"]),
            float(kline["c"]),
            float(kline["v"]),
            int(kline["n"]),
        ),
    )

    conn.commit()
    return True


async def collect():
    print("BINANCE-SUP BTC COLLECTOR STARTED", flush=True)
    print("Connecting to PostgreSQL...", flush=True)

    with psycopg.connect(DATABASE_URL) as conn:
        print("POSTGRESQL CONNECTED", flush=True)

        prepare_database(conn)

        print("TABLE btc_market_data READY", flush=True)
        print("TABLE btc_klines_1m READY", flush=True)

        while True:
            try:
                print("Connecting to Binance Futures...", flush=True)

                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    print("CONNECTED TO BINANCE FUTURES", flush=True)

                    latest_bid = None
                    latest_ask = None

                    trade_count = 0
                    trade_volume = 0.0
                    buy_volume = 0.0
                    sell_volume = 0.0

                    last_save_time = time.time()

                    async for message in websocket:
                        message_data = json.loads(message)

                        stream = message_data["stream"]
                        data = message_data["data"]
                        if not stream.endswith("@bookTicker"):
                            print(f"DEBUG OTHER STREAM: {stream}", flush=True)

                        if stream.endswith("@bookTicker"):
                            latest_bid = float(data["b"])
                            latest_ask = float(data["a"])

                        elif stream.endswith("@aggTrade"):
                            quantity = float(data["q"])

                            trade_count += 1
                            trade_volume += quantity

                            # m=True: покупатель был maker,
                            # значит агрессором был продавец.
                            if data["m"]:
                                sell_volume += quantity
                            else:
                                buy_volume += quantity

                        elif stream.endswith("@kline_1m"):
                            kline = data["k"]

                            if save_kline(conn, kline):
                                print(
                                    f"KLINE SAVED | "
                                    f"open={kline['o']} | "
                                    f"high={kline['h']} | "
                                    f"low={kline['l']} | "
                                    f"close={kline['c']} | "
                                    f"volume={kline['v']}",
                                    flush=True,
                                )

                        current_time = time.time()

                        if (
                            current_time - last_save_time >= SAVE_INTERVAL
                            and latest_bid is not None
                            and latest_ask is not None
                        ):
                            mid = (latest_bid + latest_ask) / 2
                            spread = latest_ask - latest_bid

                            save_market_snapshot(
                                conn,
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
