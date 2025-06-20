import asyncio
from fastapi import FastAPI
import requests
import re
from datetime import datetime
from dotenv import load_dotenv
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pydantic import BaseModel

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
        response = session.get(base_url, params=params, timeout=3)
        response.raise_for_status()
        data = response.json()
        if "Error Message" in data or "Information" in data or "Note" in data:
            return None
        return data
    except Exception:
        return None

def fetch_yahoo_data(ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1m&includeAdjustedClose=true"
    try:
        response = session.get(url, timeout=3)
        response.raise_for_status()
        data = response.json()
        result = data.get("chart", {}).get("result", [{}])[0]
        if not result or not result.get("timestamp") or not result.get("indicators", {}).get("adjclose"):
            return None
        timestamps = result["timestamp"]
        prices = result["indicators"]["adjclose"][0]["adjclose"]
        meta = result["meta"]
        dates = [datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") for ts in timestamps]
        return {
            "dates": dates,
            "prices": [parse_float(p) for p in prices if p is not None],
            "currency": meta.get("currency", "USD")
        }
    except Exception:
        return None

def fetch_forex_rate(from_currency, to_currency):
    if from_currency == to_currency:
        return 1.0
    cache_key = f"{from_currency}_{to_currency}"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < 3600:
        return cached_data["rate"]
    params = {"from_symbol": from_currency, "to_symbol": to_currency}
    data = fetch_alpha_vantage_data("CURRENCY_EXCHANGE_RATE", params)
    if data and "Realtime Currency Exchange Rate" in data:
        rate = parse_float(data["Realtime Currency Exchange Rate"].get("5. Exchange Rate", "N/A"))
        if rate != "N/A":
            cache[cache_key] = {"rate": rate, "timestamp": current_time}
            return rate
    return 1.0

async def scrape_quote(ticker: str):
    cache_key = f"{ticker}_quote"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        return cached_data["data"]

    is_foreign = bool(re.match(r'.*\.|^\d', ticker))
    if is_foreign:
        await asyncio.sleep(0.05)
        yahoo_data = fetch_yahoo_data(ticker)
        if yahoo_data:
            latest_price = parse_float(yahoo_data["prices"][0]) if yahoo_data["prices"] else "N/A"
            if latest_price != "N/A" and yahoo_data["currency"] != "USD":
                rate = fetch_forex_rate(yahoo_data["currency"], "USD")
                latest_price *= rate
            result = {
                "ticker": ticker,
                "latest_date": yahoo_data["dates"][0] if yahoo_data["dates"] else "N/A",
                "latest_price": latest_price
            }
            cache[cache_key] = {"data": result, "timestamp": current_time}
            return result
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

class TickerRequest(BaseModel):
    ticker: str

@app.post("/scrape_quote")
async def scrape_quote_single(request: TickerRequest):
    async with semaphore:
        return await scrape_quote(request.ticker)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
