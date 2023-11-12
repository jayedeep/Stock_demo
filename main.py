from flask import Flask, request, jsonify
import requests
import psycopg2
import json
from flask_cors import CORS  # Import CORS from flask_cors
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

api_key = os.getenv('api_key')
alpha_vantage_endpoint = 'https://www.alphavantage.co/query'

# PostgreSQL database connection
conn = psycopg2.connect(
    host=os.getenv('host'),
    database=os.getenv('database'),
    user=os.getenv('user'),
    password=os.getenv('password')
)
cursor = conn.cursor()

def create_stock_table():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            date DATE,
            symbol VARCHAR(10),
            close_price NUMERIC,
            CONSTRAINT unique_date_symbol UNIQUE (date, symbol)
        )
    ''')
    conn.commit()

def fetch_stock_data(symbol):
    print(symbol)
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key
    }

    response = requests.get(alpha_vantage_endpoint, params=params)
    data = response.json()
    if 'Time Series (Daily)' not in data:
        print("Error: Unable to fetch data.")
        return None

    time_series = data['Time Series (Daily)']
    stock_data = [{'date': date, 'symbol': symbol, 'close_price': float(entry['4. close'])} for date, entry in time_series.items()]
    return stock_data

def insert_stock_data(stock_data):
    print(stock_data, ">>>>>>stock_data")
    cursor.executemany('''
        INSERT INTO stocks (date, symbol, close_price)
        VALUES (%(date)s, %(symbol)s, %(close_price)s)
        ON CONFLICT (date, symbol) DO UPDATE
         SET close_price = EXCLUDED.close_price
    ''', stock_data)
    conn.commit()

@app.route('/insert_and_return_data', methods=['POST'])
def insert_and_return_data():
    stock_symbol = request.json.get('symbol')
    print(stock_symbol,">>>>>>>", request.json)

    create_stock_table()

    stock_data = fetch_stock_data(stock_symbol)

    if stock_data:
        insert_stock_data(stock_data)

    # Fetch the data after insertion
    cursor.execute('SELECT date,symbol, close_price FROM stocks WHERE symbol=%s', (stock_symbol,))
    print(">>>>>>>",'SELECT date, symbol, close_price FROM stocks WHERE symbol=%s', (stock_symbol,))
    data = cursor.fetchall()
    print(data,">>>>>>>>>>data")
    response_data = { 'metadata':{'symbol': stock_symbol},'data':[{'date': str(date), 'close_price':close_price} for date, symbol, close_price in data]}

    return jsonify(response_data)

if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0', port=5000)
