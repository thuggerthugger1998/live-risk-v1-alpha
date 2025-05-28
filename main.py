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