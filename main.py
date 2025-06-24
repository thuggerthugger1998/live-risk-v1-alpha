import asyncio
from fastapi import FastAPI
import requests
import re
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pydantic import BaseModel
import yfinance as yf
import yahooquery as yq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Load environment variables
load_dotenv()
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# User-agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)...",
    "Mozilla/5.0 (X11; Linux x86_64)...",
]

# Cache + Config
cache = {}
CACHE_DURATION = 60
MAX_REQUESTS_PER_MIN = 75
current_request_count = 0
last_request_time = datetime.now(timezone.utc)

# HTTP session with retry logic
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
semaphore = asyncio.Semaphore(MAX_REQUESTS_PER_MIN)

# Utility: parse float

def parse_float(value):
    try:
        return float(re.sub(r'[^0-9.]', '', str(value)))
    except:
        return "N/A"

# Utility: backoff logic
async def backoff_sleep(attempt):
    await asyncio.sleep(min(2 ** attempt, 10))

# Utility: rate limiter with quota tracker
async def throttle_requests():
    global current_request_count, last_request_time
    now = datetime.now(timezone.utc)
    if (now - last_request_time).seconds >= 60:
        current_request_count = 0
        last_request_time = now
    if current_request_count >= MAX_REQUESTS_PER_MIN:
        logger.warning("Rate limit reached, sleeping 1s")
        await asyncio.sleep(1)
        return await throttle_requests()
    current_request_count += 1
    return

# Earnings logic
async def scrape_earnings(ticker: str):
    cache_key = f"{ticker}_earnings"
    current_time = datetime.now(timezone.utc)
    if cache_key in cache and (current_time - cache[cache_key]["timestamp"]).total_seconds() < CACHE_DURATION:
        return cache[cache_key]["data"]

    earnings_date = None
    attempt = 0
    
    while attempt < 3:
        try:
            await throttle_requests()
            headers = {'User-Agent': random.choice(USER_AGENTS)}

            # Method 1: yfinance calendar
            stock = yf.Ticker(ticker)
            cal = stock.calendar
            if cal is not None and 'Earnings Date' in cal.index:
                earnings_date = cal.loc['Earnings Date'].tolist()[0]

            # Method 2: yfinance.get_earnings_dates()
            if not earnings_date:
                df = stock.get_earnings_dates(limit=12).reset_index()
                df = df[df['Earnings Date'] > datetime.now(timezone.utc)]
                if not df.empty:
                    earnings_date = df['Earnings Date'].iloc[0]

            # Method 3: yahooquery fallback
            if not earnings_date:
                stock_yq = yq.Ticker(ticker)
                calendar = stock_yq.calendar_events or stock_yq.events
                if calendar:
                    for sym_data in calendar.values():
                        earnings_data = sym_data.get("earnings")
                        if isinstance(earnings_data, list):
                            future_dates = [
                                e['startdatetime']
                                for e in earnings_data
                                if 'startdatetime' in e and datetime.fromisoformat(e['startdatetime'].replace('Z', '+00:00')) > current_time
                            ]
                            if future_dates:
                                earnings_date = datetime.fromisoformat(min(future_dates).replace('Z', '+00:00'))
                                break

            if earnings_date and earnings_date.tzinfo is None:
                earnings_date = earnings_date.replace(tzinfo=timezone.utc)

            if earnings_date and earnings_date > current_time:
                result = {"ticker": ticker, "earningsDate": earnings_date.isoformat()}
                cache[cache_key] = {"data": result, "timestamp": current_time}
                return result

            break

        except Exception as e:
            logger.error(f"Attempt {attempt+1}: error fetching for {ticker}: {e}")
            await backoff_sleep(attempt)
            attempt += 1

    return {"ticker": ticker, "earningsDate": None}

# Quote logic
async def scrape_quote(ticker: str):
    cache_key = f"{ticker}_quote"
    current_time = datetime.now(timezone.utc)
    if cache_key in cache and (current_time - cache[cache_key]["timestamp"]).total_seconds() < CACHE_DURATION:
        return cache[cache_key]["data"]

    await throttle_requests()

    try:
        if bool(re.match(r'.*\.\w+$', ticker)):
            return {"ticker": ticker, "latest_date": "N/A", "latest_price": "N/A"}

        params = {"symbol": ticker, "function": "GLOBAL_QUOTE", "apikey": ALPHA_VANTAGE_API_KEY}
        response = session.get("https://www.alphavantage.co/query", params=params)
        data = response.json().get("Global Quote", {})
        result = {
            "ticker": ticker,
            "latest_date": data.get("07. latest trading day", "N/A"),
            "latest_price": parse_float(data.get("05. price", "N/A"))
        }
        cache[cache_key] = {"data": result, "timestamp": current_time}
        return result
    except Exception as e:
        logger.error(f"Error fetching quote for {ticker}: {e}")
        return {"ticker": ticker, "latest_date": "N/A", "latest_price": "N/A"}

# Input schema
class TickerRequest(BaseModel):
    ticker: str

@app.post("/scrape_quote")
async def scrape_quote_single(request: TickerRequest):
    async with semaphore:
        return await scrape_quote(request.ticker)

@app.post("/scrape_earnings")
async def scrape_earnings_single(request: TickerRequest):
    async with semaphore:
        return await scrape_earnings(request.ticker)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
