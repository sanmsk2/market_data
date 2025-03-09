import yfinance as yf
import pandas as pd
import os

#Download a specific stock from yahoo finance api
def download_stock_data(ticker, filename):
    data=yf.Ticker(ticker)
    data_hist=data.history(period="1mo")
    data_hist.to_csv(filename)
    print(f"Stock data stored in {filename}")

#Main function to both
def main():
    filepath = "/home/ec2-user/project/datasets/"
    stockslst = ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD", "ORCL", "SNOW"]
    for ticker in stockslst:
        filename = f"{filepath}{ticker}.csv"
        download_stock_data(ticker,filename)

if __name__ == "__main__":
    main()
