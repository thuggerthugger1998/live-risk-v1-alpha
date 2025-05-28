from fastapi import FastAPI
import requests
import re
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import csv
from io import StringIO

# Configure logging to both console and file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Log the API keys to verify they’re loaded
logger.info(f"Alpha Vantage API Key: {os.getenv('ALPHA_VANTAGE_API_KEY')}")
logger.info(f"FMP API Key: {os.getenv('FMP_API_KEY')}")

app = FastAPI()

# API keys from .env
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")

# Cache for storing recent data to reduce API calls
cache = {}
full_csv_cache = None
full_csv_timestamp = None
CACHE_DURATION = 60  # Cache for 60 seconds

# Set up requests session with retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

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
        logger.info(f"Making request to {base_url} with params: {params}")
        response = session.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Response headers: {response.headers}")
        if endpoint == "EARNINGS_CALENDAR":
            # Handle CSV response
            logger.info(f"Raw CSV response: {response.text!r}")
            return response.text
        data = response.json()
        logger.info(f"Raw JSON response: {data}")
        if "Error Message" in data or "Note" in data:
            logger.error(f"API error for {endpoint}: {data}")
            return None
        return data
    except Exception as e:
        logger.error(f"Error fetching data from Alpha Vantage for {endpoint}: {e}")
        return None

def fetch_alpha_vantage_earnings(ticker):
    # Try different horizons if necessary
    horizons = ["3month", "6month", "12month"]
    for horizon in horizons:
        logger.info(f"Attempting to fetch earnings calendar with horizon: {horizon}")
        # Step 1: Try the per-symbol EARNINGS_CALENDAR endpoint
        params = {
            "symbol": ticker,
            "horizon": horizon
        }
        csv_data = fetch_alpha_vantage_data("EARNINGS_CALENDAR", params)
        logger.info(f"Step 1: Fetched per-symbol CSV data for {ticker} with horizon {horizon}: {csv_data!r}")

        if csv_data:
            # Step 2: Parse the per-symbol CSV
            try:
                lines = csv_data.strip().splitlines()
                logger.info(f"Step 2: Split per-symbol CSV into lines: {lines!r}")

                if len(lines) < 2:
                    logger.error(f"Step 3: Per-symbol CSV has no data rows (only header or empty): {lines}")
                    continue

                # Parse header and data row manually
                header = lines[0].split(',')
                data_row = lines[1].split(',')
                logger.info(f"Step 4: Header: {header!r}")
                logger.info(f"Step 5: Data row: {data_row!r}")

                # Ensure header and data row align
                if len(header) != len(data_row):
                    logger.error(f"Step 6: Mismatch between header length ({len(header)}) and data row length ({len(data_row)})")
                    continue

                # Create a dictionary from the header and data row
                row_dict = dict(zip(header, data_row))
                logger.info(f"Step 7: Parsed row as dict: {row_dict!r}")

                # Extract the reportDate
                report_date = row_dict.get("reportDate", "").strip()
                logger.info(f"Step 8: Extracted reportDate: {report_date!r}")

                if not report_date:
                    logger.error(f"Step 9: No reportDate found in row: {row_dict}")
                    continue

                # Parse the reportDate (MM/DD/YY format)
                try:
                    report_datetime = datetime.strptime(report_date, "%m/%d/%y")
                    if report_datetime.year < 2000:
                        report_datetime = report_datetime.replace(year=report_datetime.year + 100)
                    report_datetime = report_datetime.replace(tzinfo=timezone.utc)
                    logger.info(f"Step 10: Parsed reportDate as datetime: {report_datetime}")
                except ValueError as e:
                    logger.error(f"Step 11: Failed to parse reportDate {report_date!r}: {e}")
                    continue

                # Compare with current time
                current_time = datetime.now(timezone.utc)
                report_date_formatted = report_datetime.strftime("%Y-%m-%d")
                logger.info(f"Step 12: Comparing report date {report_date_formatted} ({report_datetime}) with current date {current_time}")

                if report_datetime > current_time:
                    logger.info(f"Step 13: Found upcoming earnings date for {ticker}: {report_date_formatted}")
                    return report_date_formatted
                else:
                    logger.warning(f"Step 14: Report date {report_date_formatted} is not in the future for {ticker}")
                    continue
            except Exception as e:
                logger.error(f"Step 15: Error parsing per-symbol CSV for {ticker} with horizon {horizon}: {e}")
                continue

    # Step 16: Fallback to full earnings calendar CSV
    logger.info(f"Step 16: Falling back to full earnings calendar CSV for {ticker}")
    global full_csv_cache, full_csv_timestamp
    current_time = datetime.now(timezone.utc)

    # Try different horizons for the full CSV
    for horizon in horizons:
        # Check if the full CSV is cached and still valid
        if full_csv_cache and full_csv_timestamp and (current_time - full_csv_timestamp).total_seconds() < CACHE_DURATION:
            logger.info(f"Step 17: Using cached full earnings calendar CSV")
            csv_data = full_csv_cache
        else:
            params = {
                "horizon": horizon
            }
            csv_data = fetch_alpha_vantage_data("EARNINGS_CALENDAR", params)
            logger.info(f"Step 18: Fetched full earnings calendar CSV with horizon {horizon}: {len(csv_data) if csv_data else 0} bytes")
            if csv_data:
                full_csv_cache = csv_data
                full_csv_timestamp = current_time
            else:
                logger.error(f"Step 19: Failed to fetch full earnings calendar CSV with horizon {horizon}")
                continue

        # Parse the full CSV
        try:
            lines = csv_data.strip().splitlines()
            logger.info(f"Step 20: Split full CSV into {len(lines)} lines")

            if len(lines) < 2:
                logger.error(f"Step 21: Full CSV has no data rows (only header or empty)")
                continue

            csv_reader = csv.DictReader(lines)
            for row in csv_reader:
                if row.get("symbol") == ticker:
                    logger.info(f"Step 22: Found row for {ticker}: {row}")
                    report_date = row.get("reportDate", "").strip()
                    logger.info(f"Step 23: Extracted reportDate: {report_date!r}")

                    if not report_date:
                        logger.error(f"Step 24: No reportDate found for {ticker}")
                        continue

                    # Parse the reportDate (MM/DD/YY format)
                    try:
                        report_datetime = datetime.strptime(report_date, "%m/%d/%y")
                        if report_datetime.year < 2000:
                            report_datetime = report_datetime.replace(year=report_datetime.year + 100)
                        report_datetime = report_datetime.replace(tzinfo=timezone.utc)
                        logger.info(f"Step 25: Parsed reportDate as datetime: {report_datetime}")
                    except ValueError as e:
                        logger.error(f"Step 26: Failed to parse reportDate {report_date!r}: {e}")
                        continue

                    # Compare with current time
                    current_time = datetime.now(timezone.utc)
                    report_date_formatted = report_datetime.strftime("%Y-%m-%d")
                    logger.info(f"Step 27: Comparing report date {report_date_formatted} ({report_datetime}) with current date {current_time}")

                    if report_datetime > current_time:
                        logger.info(f"Step 28: Found upcoming earnings date for {ticker}: {report_date_formatted}")
                        return report_date_formatted
                    else:
                        logger.warning(f"Step 29: Report date {report_date_formatted} is not in the future for {ticker}")
                        continue
            logger.warning(f"Step 30: No upcoming earnings date found for {ticker} in full CSV with horizon {horizon}")
        except Exception as e:
            logger.error(f"Step 31: Error parsing full earnings calendar CSV for {ticker} with horizon {horizon}: {e}")
            continue

    logger.error(f"Step 32: Failed to find upcoming earnings date for {ticker} after all attempts")
    return "N/A"

@app.get("/debug-earnings/{ticker}")
async def debug_earnings(ticker: str):
    # Debug endpoint to fetch raw CSV data
    # Try per-symbol first
    params = {
        "symbol": ticker,
        "horizon": "3month"
    }
    per_symbol_csv = fetch_alpha_vantage_data("EARNINGS_CALENDAR", params)

    # Try full CSV
    params = {
        "horizon": "3month"
    }
    full_csv = fetch_alpha_vantage_data("EARNINGS_CALENDAR", params)

    return {
        "per_symbol_csv": per_symbol_csv,
        "full_csv_first_1000_chars": full_csv[:1000] if full_csv else None
    }

def fetch_fmp_short_interest(ticker):
    # Placeholder: Short interest data requires a paid FMP plan
    logger.warning(f"Short interest data for {ticker} requires a paid FMP plan")
    return {"short_interest": "N/A", "float_shares": "N/A"}

def get_historical_data(ticker):
    params = {
        "symbol": ticker,
        "interval": "1min",
        "outputsize": "compact"
    }
    data = fetch_alpha_vantage_data("TIME_SERIES_INTRADAY", params)
    if data and "Time Series (1min)" in data:
        time_series = data["Time Series (1min)"]
        dates = []
        prices = []
        for timestamp, values in sorted(time_series.items(), reverse=True):
            price = parse_float(values["4. close"])
            if price != "N/A":
                dates.append(timestamp)
                prices.append(price)
        logger.info(f"Intraday timestamps for {ticker}: {dates[:5]}")
        return {"dates": dates, "prices": prices}
    return {"dates": [], "prices": []}

def get_earnings_date(ticker):
    return fetch_alpha_vantage_earnings(ticker)

def get_short_interest_data(ticker):
    return fetch_fmp_short_interest(ticker)

def get_company_overview(ticker):
    params = {"symbol": ticker}
    data = fetch_alpha_vantage_data("OVERVIEW", params)
    if data:
        return {
            "market_cap": parse_float(data.get("MarketCapitalization", "N/A")),
            "shares": parse_float(data.get("SharesOutstanding", "N/A"))
        }
    return {"market_cap": "N/A", "shares": "N/A"}

def get_historical_data_daily(ticker):
    params = {
        "symbol": ticker,
        "outputsize": "full",
        "function": "TIME_SERIES_DAILY"
    }
    data = fetch_alpha_vantage_data("TIME_SERIES_DAILY", params)
    if data and "Time Series (Daily)" in data:
        time_series = data["Time Series (Daily)"]
        dates = []
        prices = []
        for timestamp, values in sorted(time_series.items(), reverse=True)[:252]:
            price = parse_float(values["4. close"])
            if price != "N/A":
                dates.append(timestamp)
                prices.append(price)
        return {"dates": dates, "prices": prices}
    return {"dates": [], "prices": []}

@app.get("/scrape/{ticker}")
async def scrape_ticker(ticker: str):
    # Check cache
    cache_key = f"{ticker}_data"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        logger.info(f"Returning cached data for {ticker}")
        return cached_data["data"]

    logger.info(f"Fetching data for ticker: {ticker}")

    # Fetch historical data
    historical_data = get_historical_data(ticker)
    historical_dates = historical_data["dates"]
    prices = historical_data["prices"]

    # Fetch earnings date using Alpha Vantage
    next_report_date = get_earnings_date(ticker)

    # Fetch short interest data using FMP
    short_data = get_short_interest_data(ticker)
    short_interest = short_data["short_interest"]
    float_shares = short_data["float_shares"]

    # Calculate short_percent_of_float
    short_percent = "N/A"
    if short_interest != "N/A" and float_shares != "N/A" and float_shares != 0:
        short_percent = round((short_interest / float_shares) * 100, 2)

    # Fetch company overview for market cap and shares (only for non-ETFs)
    market_cap = "N/A"
    if ticker != "SPY":
        overview_data = get_company_overview(ticker)
        market_cap = overview_data["market_cap"]

    # Fetch daily historical data for future Beta calculations
    historical_data_daily = get_historical_data_daily(ticker)
    historical_dates_daily = historical_data_daily["dates"]
    prices_daily = historical_data_daily["prices"]

    # Calculate days_to_cover
    days_to_cover = "N/A"

    # Calculate metrics
    daily_returns = []
    for j in range(1, min(len(prices_daily), 31)):
        if prices_daily[j] and prices_daily[j - 1]:
            daily_returns.append(prices_daily[j] / prices_daily[j - 1] - 1)
    daily_volatility = calculate_standard_deviation(daily_returns) if daily_returns else "N/A"
    if daily_volatility != "N/A":
        daily_volatility = round(daily_volatility, 4)

    sma50 = calculate_sma(prices_daily[:50], 50) if len(prices_daily) >= 50 else "N/A"
    if sma50 != "N/A":
        sma50 = round(sma50, 2)
    sma200 = calculate_sma(prices_daily[:200], 200) if len(prices_daily) >= 200 else "N/A"
    if sma200 != "N/A":
        sma200 = round(sma200, 2)

    rsi14 = calculate_rsi(prices_daily[:15], 14) if len(prices_daily) >= 15 else "N/A"
    if rsi14 != "N/A":
        rsi14 = round(rsi14, 2)

    annual_returns = []
    for j in range(1, min(len(prices_daily), 253)):
        if prices_daily[j] and prices_daily[j - 1]:
            annual_returns.append(prices_daily[j] / prices_daily[j - 1] - 1)
    annual_volatility = calculate_standard_deviation(annual_returns) * (252 ** 0.5) if annual_returns else "N/A"
    if annual_volatility != "N/A":
        annual_volatility = round(annual_volatility, 4)

    # Cache the result
    result = {
        "ticker": ticker,
        "next_report_date": next_report_date,
        "short_interest": short_interest,
        "short_percent_of_float": str(short_percent),
        "days_to_cover": str(days_to_cover),
        "volatility_daily": str(daily_volatility),
        "sma_50d": str(sma50),
        "sma_200d": str(sma200),
        "rsi_14d": str(rsi14),
        "volatility_annualized": str(annual_volatility),
        "historical_dates": historical_dates,
        "historical_prices": prices,
        "historical_dates_daily": historical_dates_daily,
        "historical_prices_daily": prices_daily
    }
    # Include market_cap only for non-ETFs
    if ticker != "SPY":
        result["market_cap"] = market_cap

    cache[cache_key] = {"data": result, "timestamp": current_time}

    return result

def calculate_standard_deviation(returns):
    if len(returns) < 2:
        return "N/A"
    mean = sum(returns) / len(returns)
    variance = sum((x - mean) ** 2 for x in returns) / (len(returns) - 1)
    return (variance ** 0.5)

def calculate_sma(prices, period):
    if len(prices) < period:
        return "N/A"
    return sum(prices[:period]) / period

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return "N/A"
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i - 1]
        gains.append(change if change > 0 else 0)
        losses.append(abs(change) if change < 0 else 0)
    
    if len(gains) < period:
        return "N/A"
    
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)