from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)


def connect_db():
    conn = sqlite3.connect('finance_data.db')
    conn.row_factory = sqlite3.Row
    return conn

# API to get all companies' stock data for a particular day
@app.route('/api/stocks/all/<string:date>', methods=['GET'])
def get_all_stocks_by_date(date):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM finance_data WHERE date=?", (date,))
    rows = cur.fetchall()
    data = []
    for row in rows:
        data.append(dict(row))
    conn.close()
    return jsonify(data)

# API to get all stock data for a particular company for a particular day
@app.route('/api/stocks/<string:company>/<string:date>', methods=['GET'])
def get_company_stocks_by_date(company, date):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM finance_data WHERE company=? AND date=?", (company, date))
    rows = cur.fetchall()
    data = []
    for row in rows:
        data.append(dict(row))
    conn.close()
    return jsonify(data)

# API to get all stock data for a particular company
@app.route('/api/stocks/<string:company>', methods=['GET'])
def get_company_stocks(company):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM finance_data WHERE company=?", (company,))
    rows = cur.fetchall()
    data = []
    for row in rows:
        data.append(dict(row))
    conn.close()
    return jsonify(data)

# API to update stock data for a company by date
@app.route('/api/stocks/<string:company>/<string:date>', methods=['POST', 'PATCH'])
def update_company_stocks(company, date):
    data = request.json
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM finance_data WHERE company=? AND date=?", (company, date))
    row = cur.fetchone()
    if row:
        cur.execute("UPDATE finance_data SET open=?, high=?, low=?, close=?, adj_close=?, volume=? WHERE company=? AND date=?",
                    (data['open'], data['high'], data['low'], data['close'], data['adj_close'], data['volume'], company, date))
    else:
        cur.execute("INSERT INTO finance_data (company, date, open, high, low, close, adj_close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (company, date, data['open'], data['high'], data['low'], data['close'], data['adj_close'], data['volume']))
    conn.commit()
    conn.close()
    return "Stock data for {} on {} has been updated".format(company, date)

if __name__ == '__main__':
    app.run(debug=True)
