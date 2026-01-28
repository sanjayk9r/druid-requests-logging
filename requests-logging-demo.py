from flask import Flask, render_template, request
from pydruid.db import connect
import json

app = Flask(__name__)

# Configuration - Update with your Druid Broker details
DRUID_HOST = 'localhost'
DRUID_PORT = 8888
DRUID_SCHEME = 'http'

def get_druid_connection():
    return connect(host=DRUID_HOST, port=DRUID_PORT, path='/druid/v2/sql/', scheme=DRUID_SCHEME, user="myuser", password="mysupersecret")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def query():
    search_term = request.form.get('search_term', '')
    results = []
    error = None

    try:
        conn = get_druid_connection()
        curs = conn.cursor()

        sql_query = f"SELECT \"native_query\", \"sql\" FROM druid_query_performance WHERE queryId = \'{search_term}\'"
        curs.execute(sql_query)
        
        for row in curs:
            # Attempt to format the JSON string for better UI display
            try:
                formatted_json = json.dumps(json.loads(row[0]), indent=2)
            except:
                formatted_json = row[0]
            results.append({
                'native_json': formatted_json,
                'sql': row[1]})
            
    except Exception as e:
        error = str(e)
    finally:
        if 'conn' in locals():
            conn.close()

    return render_template('index.html', results=results, error=error, search_term=search_term, queryId=search_term)

if __name__ == '__main__':
    app.run(debug=True)
