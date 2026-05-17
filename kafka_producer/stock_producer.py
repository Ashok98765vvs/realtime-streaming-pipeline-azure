import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import yfinance as yf
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("StockProducer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-events")
STOCKS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX"]
POLL_INTERVAL_SECONDS = 5


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
    )


def fetch_stock_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.fast_info
        return {
            "ticker": ticker,
            "timestamp": datetime.utcnow().isoformat(),
            "open": round(float(info.open or 0), 4),
            "high": round(float(info.day_high or 0), 4),
            "low": round(float(info.day_low or 0), 4),
            "current_price": round(float(info.last_price or 0), 4),
            "volume": int(info.last_volume or 0),
            "market_cap": int(info.market_cap or 0),
            "52_week_high": round(float(info.year_high or 0), 4),
            "52_week_low": round(float(info.year_low or 0), 4),
            "event_type": "STOCK_TICK",
            "source": "yahoo_finance",
        }
    except Exception as e:
        logger.warning(f"Failed to fetch {ticker}: {e}")
        return None


def run_producer():
    producer = create_producer()
    logger.info(f"Producer started | Topic: {KAFKA_TOPIC} | Stocks: {STOCKS}")
    events_sent = 0
    try:
        while True:
            batch_start = time.time()
            for ticker in STOCKS:
                event = fetch_stock_data(ticker)
                if event:
                    producer.send(KAFKA_TOPIC, key=ticker, value=event)
                    events_sent += 1
                    logger.info(f"{ticker}: ${event['current_price']} | Vol: {event['volume']:,}")
            producer.flush()
            elapsed = time.time() - batch_start
            logger.info(f"Batch complete — {len(STOCKS)} events | Total: {events_sent} | {elapsed:.2f}s")
            time.sleep(max(0, POLL_INTERVAL_SECONDS - elapsed))
    except KeyboardInterrupt:
        logger.info("Producer stopped")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()
