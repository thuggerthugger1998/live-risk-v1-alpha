import asyncio
from fastapi import FastAPI
import requests
import re
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pydantic import BaseModel
import yfinance as yf

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Load environment variables
load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# Initialize cache
cache = {}
CACHE_DURATION = 10

session = requests.Session()
retries = Retry(total=3, backoff_factor=0.2, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

semaphore = asyncio.Semaphore(75)

def parse_float(value):
    if not value or value == "N/A":
        return "N/A"
    try:
        if isinstance(value, str):
            cleaned_value = re.sub(r'[^0-9.]', '', value)
            return float(cleaned_value)
        return float(value)
    except:
        return "N/A"

def fetch_alpha_vantage_data(endpoint, params):
    base_url = "https://www.alphavantage.co/query"
    params["apikey"] = ALPHA_VANTAGE_API_KEY
    params["function"] = endpoint
    try:
        response = session.get(base_url, params=params, timeout=5)
        response.raise_for_status()
        data = response.json()
        if "Error Message" in data or "Information" in data or "Note" in data:
            logger.error(f"Alpha Vantage error: {data.get('Error Message', '') or data.get('Information', '') or data.get('Note', '')}")
            return None
        return data
    except Exception as e:
        logger.error(f"Error fetching Alpha Vantage data: {e}")
        return None

async def scrape_quote(ticker: str):
    cache_key = f"{ticker}_quote"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        return cached_data["data"]

    # Skip international tickers
    is_foreign = bool(re.match(r'.*\.|^\d', ticker))
    if is_foreign:
        return {
            "ticker": ticker,
            "latest_date": "N/A",
            "latest_price": "N/A"
        }

    await asyncio.sleep(0.05)
    params = {"symbol": ticker}
    data = fetch_alpha_vantage_data("GLOBAL_QUOTE", params)
    if data and "Global Quote" in data:
        quote = data["Global Quote"]
        latest_price = parse_float(quote.get("05. price", "N/A"))
        latest_date = quote.get("07. latest trading day", "N/A")
        result = {
            "ticker": ticker,
            "latest_date": latest_date,
            "latest_price": latest_price
        }
        if latest_price != "N/A":
            cache[cache_key] = {"data": result, "timestamp": current_time}
        return result
    return {
        "ticker": ticker,
        "latest_date": "N/A",
        "latest_price": "N/A"
    }

async def scrape_earnings(ticker: str):
    cache_key = f"{ticker}_earnings"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        return cached_data["data"]

    try:
        stock = yf.Ticker(ticker)
        earnings_dates = stock.calendar
        if earnings_dates is not None and 'Earnings Date' in earnings_dates:
            earnings_date = earnings_dates['Earnings Date'][0]
            if earnings_date > datetime.now(timezone.utc):
                result = {
                    "ticker": ticker,
                    "earningsDate": earnings_date.isoformat()
                }
                cache[cache_key] = {"data": result, "timestamp": current_time}
                return result
        return {
            "ticker": ticker,
            "earningsDate": None
        }
    except Exception as e:
        logger.error(f"Error fetching earnings for {ticker}: {e}")
        return {
            "ticker": ticker,
            "earningsDate": None,
            "error": str(e)
        }

class TickerRequest(BaseModel):
    ticker: str

@app.post("/scrape_quote")
async def scrape_quote_single(request: TickerRequest):
    async with semaphore:
        try:
            return await scrape_quote(request.ticker)
        except Exception as e:
            logger.error(f"Error processing {request.ticker}: {e}")
            return {
                "ticker": request.ticker,
                "latest_date": "N/A",
                "latest_price": "N/A",
                "error": str(e)
            }

@app.post("/scrape_earnings")
async def scrape_earnings_single(request: TickerRequest):
    async with semaphore:
        try:
            return await scrape_earnings(request.ticker)
        except Exception as e:
            logger.error(f"Error processing earnings for {request.ticker}: {e}")
            return {
                "ticker": request.ticker,
                "earningsDate": None,
                "error": str(e)
            }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
