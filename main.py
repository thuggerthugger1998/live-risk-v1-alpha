import asyncio
from fastapi import FastAPI, HTTPException
import requests
import re
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import csv
from io import StringIO
from pydantic import BaseModel
from typing import List

# Configure logging
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

logger.info(f"Alpha Vantage API Key: {os.getenv('ALPHA_VANTAGE_API_KEY')}")
logger.info(f"FMP API Key: {os.getenv('FMP_API_KEY')}")

app = FastAPI()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")

cache = {}
full_csv_cache = None
full_csv_timestamp = None
CACHE_DURATION = 10

session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

sem = asyncio.Semaphore(75)

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
    max_attempts = 3
    attempt = 1
    while attempt <= max_attempts:
        try:
            logger.info(f"Attempt {attempt}: Making request to {base_url} with params: {params}")
            response = session.get(base_url, params=params, timeout=10)
            response.raise_for_status()
            if endpoint == "EARNINGS_CALENDAR":
                return response.text
            data = response.json()
            if "Error Message" in data:
                logger.error(f"API error for {endpoint}: {data['Error Message']}")
                return None
            if "Information" in data and "Thank you for using Alpha Vantage" in data["Information"]:
                logger.error(f"Rate limit exceeded for {endpoint}")
                if attempt < max_attempts:
                    logger.info(f"Retrying after 30 seconds...")
                    time.sleep(30)
                    attempt += 1
                    continue
                return None
            if "Note" in data:
                logger.error(f"API note for {endpoint}: {data['Note']}")
                if attempt < max_attempts:
                    logger.info(f"Retrying after 30 seconds...")
                    time.sleep(30)
                    attempt += 1
                    continue
                return None
            logger.info(f"Successfully fetched data for {endpoint}")
            return data
        except Exception as e:
            logger.error(f"Error fetching data from Alpha Vantage for {endpoint}: {e}")
            if attempt < max_attempts:
                logger.info(f"Retrying after 30 seconds...")
                time.sleep(30)
                attempt += 1
                continue
            return None

def fetch_yahoo_data(ticker, range="1mo", interval="1d"):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range={range}&interval={interval}&includeAdjustedClose=true"
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        result = data.get("chart", {}).get("result", [{}])[0]
        if not result or not result.get("timestamp") or not result.get("indicators", {}).get("adjclose"):
            logger.warning(f"No valid data in Yahoo response for {ticker}")
            return None
        timestamps = result["timestamp"]
        prices = result["indicators"]["adjclose"][0]["adjclose"]
        meta = result["meta"]
        dates = [datetime.fromtimestamp(ts).strftime("%Y-%m-%d") for ts in timestamps]
        exchange_to_country = {
            'Europe/Stockholm': 'Sweden', 'Europe/London': 'United Kingdom', 'Europe/Milan': 'Italy',
            'Europe/Paris': 'France', 'Europe/Frankfurt': 'Germany', 'Australia/Sydney': 'Australia',
            'Asia/Tokyo': 'Japan', 'America/Toronto': 'Canada', 'America/New_York': 'USA'
        }
        logger.info(f"Successfully fetched Yahoo data for {ticker}")
        return {
            "dates": dates,
            "prices": [parse_float(p) for p in prices if p is not None],
            "company_name": meta.get("longName") or meta.get("shortName") or "N/A",
            "country": exchange_to_country.get(meta.get("exchangeTimezoneName"), "Unknown"),
            "currency": meta.get("currency", "USD")
        }
    except Exception as e:
        logger.error(f"Error fetching Yahoo data for {ticker}: {e}")
        return None

def fetch_alpha_vantage_earnings(ticker):
    horizons = ["3month", "6month", "12month"]
    for horizon in horizons:
        params = {"symbol": ticker, "horizon": horizon}
        csv_data = fetch_alpha_vantage_data("EARNINGS_CALENDAR", params)
        if csv_data:
            try:
                lines = csv_data.strip().splitlines()
                if len(lines) < 2:
                    continue
                header = lines[0].split(',')
                data_row = lines[1].split(',')
                if len(header) != len(data_row):
                    continue
                row_dict = dict(zip(header, data_row))
                report_date = row_dict.get("reportDate", "").strip()
                if not report_date:
                    continue
                report_datetime = datetime.strptime(report_date, "%m/%d/%y")
                if report_datetime.year < 2000:
                    report_datetime = report_datetime.replace(year=report_datetime.year + 100)
                report_datetime = report_datetime.replace(tzinfo=timezone.utc)
                current_time = datetime.now(timezone.utc)
                if report_datetime > current_time:
                    return report_datetime.strftime("%Y-%m-%d")
            except Exception as e:
                logger.error(f"Error parsing earnings CSV for {ticker}: {e}")
                continue

    for horizon in horizons:
        if full_csv_cache and full_csv_timestamp and (datetime.now(timezone.utc) - full_csv_timestamp).total_seconds() < CACHE_DURATION:
            csv_data = full_csv_cache
        else:
            params = {"horizon": horizon}
            csv_data = fetch_alpha_vantage_data("EARNINGS_CALENDAR", params)
            if csv_data:
                global full_csv_cache, full_csv_timestamp
                full_csv_cache = csv_data
                full_csv_timestamp = datetime.now(timezone.utc)
            else:
                continue
        try:
            lines = csv_data.strip().splitlines()
            if len(lines) < 2:
                continue
            csv_reader = csv.DictReader(lines)
            for row in csv_reader:
                if row.get("symbol") == ticker:
                    report_date = row.get("reportDate", "").strip()
                    if not report_date:
                        continue
                    report_datetime = datetime.strptime(report_date, "%m/%d/%y")
                    if report_datetime.year < 2000:
                        report_datetime = report_datetime.replace(year=report_datetime.year + 100)
                    report_datetime = report_datetime.replace(tzinfo=timezone.utc)
                    current_time = datetime.now(timezone.utc)
                    if report_datetime > current_time:
                        return report_datetime.strftime("%Y-%m-%d")
        except Exception as e:
            logger.error(f"Error parsing full earnings CSV for {ticker}: {e}")
            continue
    return "N/A"

def fetch_fmp_short_interest(ticker):
    return {"short_interest": "N/A", "float_shares": "N/A"}

def get_historical_data(ticker):
    is_foreign = re.match(r'.*\.(ST|PA|MI|DE|OL|AX|T|TO|L)$', ticker, re.IGNORECASE)
    logger.info(f"Processing {ticker}: {'Foreign' if is_foreign else 'US'} ticker")
    if is_foreign:
        logger.info(f"Using Yahoo Finance for {ticker}")
        yahoo_data = fetch_yahoo_data(ticker, range="1mo", interval="1min")
        if yahoo_data:
            return {"dates": yahoo_data["dates"], "prices": yahoo_data["prices"]}
        else:
            logger.warning(f"Failed to fetch Yahoo data for {ticker}")
        return {"dates": [], "prices": []}
    logger.info(f"Using Alpha Vantage for {ticker}")
    params = {"symbol": ticker, "interval": "1min", "outputsize": "compact"}
    data = fetch_alpha_vantage_data("TIME_SERIES_INTRADAY", params)
    if data and "Time Series (1min)" in data:
        time_series = data["Time Series (1min)"]
        dates = []
        prices = []
        for timestamp, values in sorted(time_series.items(), reverse=True):
            price = parse_float(values["5. adjusted close"])
            if price != "N/A":
                dates.append(timestamp)
                prices.append(price)
        if dates:
            logger.info(f"Successfully fetched Alpha Vantage intraday data for {ticker}")
        else:
            logger.warning(f"No valid intraday data for {ticker} from Alpha Vantage")
        return {"dates": dates, "prices": prices}
    logger.warning(f"Failed to fetch Alpha Vantage intraday data for {ticker}")
    return {"dates": [], "prices": []}

def get_earnings_date(ticker):
    return fetch_alpha_vantage_earnings(ticker)

def get_short_interest_data(ticker):
    return fetch_fmp_short_interest(ticker)

def get_company_overview(ticker):
    is_foreign = re.match(r'.*\.(ST|PA|MI|DE|OL|AX|T|TO|L)$', ticker, re.IGNORECASE)
    if is_foreign:
        logger.info(f"Fetching company overview from Yahoo Finance for {ticker}")
        yahoo_data = fetch_yahoo_data(ticker, range="1d", interval="1d")
        if yahoo_data:
            return {
                "market_cap": "N/A",
                "shares": "N/A",
                "company_name": yahoo_data["company_name"],
                "country": yahoo_data["country"]
            }
        logger.warning(f"Failed to fetch company overview from Yahoo Finance for {ticker}")
        return {"market_cap": "N/A", "shares": "N/A", "company_name": "N/A", "country": "N/A"}
    logger.info(f"Fetching company overview from Alpha Vantage for {ticker}")
    params = {"symbol": ticker}
    data = fetch_alpha_vantage_data("OVERVIEW", params)
    if data:
        return {
            "market_cap": parse_float(data.get("MarketCapitalization", "N/A")),
            "shares": parse_float(data.get("SharesOutstanding", "N/A")),
            "company_name": data.get("Name", "N/A"),
            "country": data.get("Country", "N/A")
        }
    logger.warning(f"Failed to fetch company overview from Alpha Vantage for {ticker}")
    return {"market_cap": "N/A", "shares": "N/A", "company_name": "N/A", "country": "N/A"}

def get_currency(ticker):
    suffix_to_currency = {
        ".ST": "SEK", ".PA": "EUR", ".MI": "EUR", ".DE": "EUR", ".OL": "NOK",
        ".AX": "AUD", ".T": "JPY", ".TO": "CAD", ".L": "GBP"
    }
    for suffix, currency in suffix_to_currency.items():
        if ticker.upper().endswith(suffix):
            return currency
    return "USD"

def fetch_forex_data(from_currency, to_currency, frequency="daily"):
    if from_currency == to_currency:
        return {"dates": [], "rates": []}
    function = {"daily": "FX_DAILY", "weekly": "FX_WEEKLY", "monthly": "FX_MONTHLY"}.get(frequency, "FX_DAILY")
    params = {"from_symbol": from_currency, "to_symbol": to_currency, "outputsize": "full"}
    data = fetch_alpha_vantage_data(function, params)
    if not data:
        return {"dates": [], "rates": []}
    time_series_key = f"Time Series FX ({frequency.capitalize()})"
    if time_series_key not in data:
        return {"dates": [], "rates": []}
    time_series = data[time_series_key]
    dates = []
    rates = []
    for date, values in sorted(time_series.items(), reverse=True):
        rate = parse_float(values.get("4. close", "N/A"))
        if rate != "N/A":
            dates.append(date)
            rates.append(rate)
    return {"dates": dates, "rates": rates}

def adjust_for_currency(ticker, dates, prices, frequency):
    currency = get_currency(ticker)
    if currency == "USD":
        return dates, prices
    forex_data = fetch_forex_data(currency, "USD", frequency)
    if not forex_data or not forex_data["dates"]:
        logger.warning(f"No forex data for {currency}/USD, using unadjusted prices for {ticker}")
        return dates, prices
    forex_dates = forex_data["dates"]
    forex_rates = forex_data["rates"]
    adjusted_prices = []
    for i, date in enumerate(dates):
        closest_rate = None
        for j, forex_date in enumerate(forex_dates):
            if forex_date <= date:
                closest_rate = forex_rates[j]
                break
        if closest_rate is None:
            closest_rate = forex_rates[-1] if forex_rates else 1.0
        adjusted_price = prices[i] * closest_rate
        adjusted_prices.append(adjusted_price)
        logger.info(f"Converted {ticker} price on {date} from {currency} to USD using rate {closest_rate}: {adjusted_price}")
    return dates, adjusted_prices

def get_historical_data_daily(ticker):
    is_foreign = re.match(r'.*\.(ST|PA|MI|DE|OL|AX|T|TO|L)$', ticker, re.IGNORECASE)
    if is_foreign:
        logger.info(f"Fetching daily data from Yahoo Finance for {ticker}")
        yahoo_data = fetch_yahoo_data(ticker, range="1mo", interval="1d")
        if yahoo_data:
            dates, prices = adjust_for_currency(ticker, yahoo_data["dates"], yahoo_data["prices"], "daily")
            return {"dates": dates, "prices": prices}
        logger.warning(f"Failed to fetch daily data from Yahoo Finance for {ticker}")
        return {"dates": [], "prices": []}
    logger.info(f"Fetching daily data from Alpha Vantage for {ticker}")
    params = {"symbol": ticker, "outputsize": "full"}
    data = fetch_alpha_vantage_data("TIME_SERIES_DAILY_ADJUSTED", params)
    if data and "Time Series (Daily)" in data:
        time_series = data["Time Series (Daily)"]
        dates = []
        prices = []
        for date, values in sorted(time_series.items(), reverse=True):
            price = parse_float(values["5. adjusted close"])
            if price != "N/A":
                dates.append(date)
                prices.append(price)
        if dates:
            logger.info(f"Successfully fetched Alpha Vantage daily data for {ticker}")
        else:
            logger.warning(f"No valid daily data for {ticker} from Alpha Vantage")
        return {"dates": dates, "prices": prices}
    logger.warning(f"Failed to fetch Alpha Vantage daily data for {ticker}")
    return {"dates": [], "prices": []}

def get_historical_data_weekly(ticker):
    is_foreign = re.match(r'.*\.(ST|PA|MI|DE|OL|AX|T|TO|L)$', ticker, re.IGNORECASE)
    if is_foreign:
        logger.info(f"Fetching weekly data from Yahoo Finance for {ticker}")
        yahoo_data = fetch_yahoo_data(ticker, range="1y", interval="1wk")
        if yahoo_data:
            dates, prices = adjust_for_currency(ticker, yahoo_data["dates"], yahoo_data["prices"], "weekly")
            return {"dates": dates, "prices": prices}
        logger.warning(f"Failed to fetch weekly data from Yahoo Finance for {ticker}")
        return {"dates": [], "prices": []}
    logger.info(f"Fetching weekly data from Alpha Vantage for {ticker}")
    params = {"symbol": ticker, "outputsize": "full"}
    data = fetch_alpha_vantage_data("TIME_SERIES_WEEKLY_ADJUSTED", params)
    if data and "Weekly Adjusted Time Series" in data:
        time_series = data["Weekly Adjusted Time Series"]
        dates = []
        prices = []
        for date, values in sorted(time_series.items(), reverse=True):
            price = parse_float(values["5. adjusted close"])
            if price != "N/A":
                dates.append(date)
                prices.append(price)
        if dates:
            logger.info(f"Successfully fetched Alpha Vantage weekly data for {ticker}")
        else:
            logger.warning(f"No valid weekly data for {ticker} from Alpha Vantage")
        return {"dates": dates, "prices": prices}
    logger.warning(f"Failed to fetch Alpha Vantage weekly data for {ticker}")
    return {"dates": [], "prices": []}

def get_historical_data_monthly(ticker):
    is_foreign = re.match(r'.*\.(ST|PA|MI|DE|OL|AX|T|TO|L)$', ticker, re.IGNORECASE)
    if is_foreign:
        logger.info(f"Fetching monthly data from Yahoo Finance for {ticker}")
        yahoo_data = fetch_yahoo_data(ticker, range="5y", interval="1mo")
        if yahoo_data:
            dates, prices = adjust_for_currency(ticker, yahoo_data["dates"], yahoo_data["prices"], "monthly")
            return {"dates": dates, "prices": prices}
        logger.warning(f"Failed to fetch monthly data from Yahoo Finance for {ticker}")
        return {"dates": [], "prices": []}
    logger.info(f"Fetching monthly data from Alpha Vantage for {ticker}")
    params = {"symbol": ticker, "outputsize": "full"}
    data = fetch_alpha_vantage_data("TIME_SERIES_MONTHLY_ADJUSTED", params)
    if data and "Monthly Adjusted Time Series" in data:
        time_series = data["Monthly Adjusted Time Series"]
        dates = []
        prices = []
        for date, values in sorted(time_series.items(), reverse=True):
            price = parse_float(values["5. adjusted close"])
            if price != "N/A":
                dates.append(date)
                prices.append(price)
        if dates:
            logger.info(f"Successfully fetched Alpha Vantage monthly data for {ticker}")
        else:
            logger.warning(f"No valid monthly data for {ticker} from Alpha Vantage")
        return {"dates": dates, "prices": prices}
    logger.warning(f"Failed to fetch Alpha Vantage monthly data for {ticker}")
    return {"dates": [], "prices": []}

async def scrape_ticker(ticker: str):
    cache_key = f"{ticker}_data"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        logger.info(f"Returning cached data for {ticker}")
        return cached_data["data"]

    logger.info(f"Scraping data for {ticker}")
    historical_data = get_historical_data(ticker)
    historical_dates = historical_data["dates"]
    prices = historical_data["prices"]

    next_report_date = get_earnings_date(ticker)
    short_data = get_short_interest_data(ticker)
    short_interest = short_data["short_interest"]
    float_shares = short_data["float_shares"]

    short_percent = "N/A"
    if short_interest != "N/A" and float_shares != "N/A" and float_shares != 0:
        short_percent = round((short_interest / float_shares) * 100, 2)

    market_cap = "N/A"
    company_name = "N/A"
    country = "N/A"
    if ticker != "SPY":
        overview_data = get_company_overview(ticker)
        market_cap = overview_data["market_cap"]
        company_name = overview_data["company_name"]
        country = overview_data["country"]

    historical_data_daily = get_historical_data_daily(ticker)
    historical_dates_daily = historical_data_daily["dates"]
    prices_daily = historical_data_daily["prices"]

    days_to_cover = "N/A"

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

    result = {
        "ticker": ticker,
        "company_name": company_name,
        "country": country,
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
    if ticker != "SPY":
        result["market_cap"] = market_cap

    if historical_dates_daily:
        cache[cache_key] = {"data": result, "timestamp": current_time}
    return result

@app.get("/scrape-weekly/{ticker}")
async def scrape_ticker_weekly(ticker: str):
    cache_key = f"{ticker}_weekly_data"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        return cached_data["data"]

    historical_data_weekly = get_historical_data_weekly(ticker)
    historical_dates_weekly = historical_data_weekly["dates"]
    prices_weekly = historical_data_weekly["prices"]

    result = {
        "ticker": ticker,
        "historical_dates_weekly": historical_dates_weekly,
        "historical_prices_weekly": prices_weekly
    }

    if historical_dates_weekly:
        cache[cache_key] = {"data": result, "timestamp": current_time}
    return result

@app.get("/scrape-monthly/{ticker}")
async def scrape_ticker_monthly(ticker: str):
    cache_key = f"{ticker}_monthly_data"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        return cached_data["data"]

    historical_data_monthly = get_historical_data_monthly(ticker)
    historical_dates_monthly = historical_data_monthly["dates"]
    prices_monthly = historical_data_monthly["prices"]

    result = {
        "ticker": ticker,
        "historical_dates_monthly": historical_dates_monthly,
        "historical_prices_monthly": prices_monthly
    }

    if historical_dates_monthly:
        cache[cache_key] = {"data": result, "timestamp": current_time}
    return result

@app.get("/forex/{from_currency}/{to_currency}/{frequency}")
async def get_forex_data(from_currency: str, to_currency: str, frequency: str):
    cache_key = f"forex_{from_currency}_{to_currency}_{frequency}"
    cached_data = cache.get(cache_key)
    current_time = datetime.now(timezone.utc)
    if cached_data and (current_time - cached_data["timestamp"]).total_seconds() < CACHE_DURATION:
        return cached_data["data"]

    forex_data = fetch_forex_data(from_currency, to_currency, frequency)
    if not forex_data["dates"]:
        raise HTTPException(status_code=404, detail="No forex data available")
    result = {
        "from_currency": from_currency,
        "to_currency": to_currency,
        "frequency": frequency,
        "dates": forex_data["dates"],
        "rates": forex_data["rates"]
    }
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

class TickerRequest(BaseModel):
    tickers: List[str]

@app.post("/scrape_batch")
async def scrape_batch(request: TickerRequest):
    async def process_ticker(ticker):
        async with sem:
            try:
                return await scrape_ticker(ticker)
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                return {
                    "ticker": ticker,
                    "company_name": "N/A",
                    "country": "N/A",
                    "historical_dates_daily": [],
                    "historical_prices_daily": [],
                    "error": str(e)
                }

    tasks = [process_ticker(ticker) for ticker in request.tickers]
    results = await asyncio.gather(*tasks)
    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
