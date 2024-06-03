import gzip
import io
import os
import sqlite3
import requests
from flask import Flask, jsonify, request
import pandas as pd
import json
from datetime import datetime, timedelta
from dateutil import parser
from sp_api.api import ReportsV2
from sp_api.base import Marketplaces
from time import sleep

app = Flask(__name__)

# Mapping of country codes to Marketplaces
marketplaces = {
    'AE': Marketplaces.AE, 'BE': Marketplaces.BE, 'DE': Marketplaces.DE, 'PL': Marketplaces.PL,
    'EG': Marketplaces.EG, 'ES': Marketplaces.ES, 'FR': Marketplaces.FR, 'GB': Marketplaces.GB,
    'IN': Marketplaces.IN, 'IT': Marketplaces.IT, 'NL': Marketplaces.NL, 'SA': Marketplaces.SA,
    'SE': Marketplaces.SE, 'TR': Marketplaces.TR, 'UK': Marketplaces.UK, 'ZA': Marketplaces.ZA,
    'AU': Marketplaces.AU, 'JP': Marketplaces.JP, 'SG': Marketplaces.SG, 'US': Marketplaces.US,
    'BR': Marketplaces.BR, 'CA': Marketplaces.CA, 'MX': Marketplaces.MX
}


def init_db():
    conn = sqlite3.connect('reports_cache.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS report_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT,
            marketplace TEXT,
            start_time TEXT,
            end_time TEXT,
            record_path TEXT,
            report_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()


init_db()


def get_cached_report_id(report_type, marketplace, start_time, end_time, record_path):
    conn = sqlite3.connect('reports_cache.db')
    c = conn.cursor()
    c.execute('''
        SELECT report_id FROM report_cache
        WHERE report_type = ? AND marketplace = ? AND start_time = ? AND end_time = ? AND record_path = ?
        ORDER BY created_at DESC LIMIT 1
    ''', (report_type, marketplace, start_time, end_time, json.dumps(record_path)))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None


def save_report_cache(report_type, marketplace, start_time, end_time, record_path, report_id):
    conn = sqlite3.connect('reports_cache.db')
    c = conn.cursor()
    c.execute('''
        INSERT INTO report_cache (report_type, marketplace, start_time, end_time, record_path, report_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (report_type, marketplace, start_time, end_time, json.dumps(record_path), report_id))
    conn.commit()
    conn.close()


def get_credentials():
    refresh_token = os.environ.get('AMAZON_EU_REFRESH_TOKEN')
    lwa_app_id = os.environ.get('AMAZON_CLIENT_ID')
    lwa_client_secret = os.environ.get('AMAZON_CLIENT_SECRET')

    if not all([refresh_token, lwa_app_id, lwa_client_secret]):
        raise ValueError("One or more required environment variables are missing.")

    return {
        'refresh_token': refresh_token,
        'lwa_app_id': lwa_app_id,
        'lwa_client_secret': lwa_client_secret,
    }


def request_report(reports_api, report_type, start_time, end_time, asingranularity='SKU', dategranularity='DAY'):
    response = reports_api.create_report(
        reportType=report_type,
        dataStartTime=start_time,
        dataEndTime=end_time,
        reportOptions={'asinGranularity': asingranularity, 'dateGranularity': dategranularity}
    )
    return response.payload['reportId']


def check_report_status(reports_api, report_id):
    status = 'IN_QUEUE'
    while status in ['IN_QUEUE', 'IN_PROGRESS']:
        sleep(30)
        report_response = reports_api.get_report(report_id)
        status = report_response.payload['processingStatus']
        print(f"Report status: {status}")
        if status == 'DONE':
            return report_response.payload['reportDocumentId']
    return None


def download_report(reports_api, document_id):
    document_response = reports_api.get_report_document(document_id)
    download_url = document_response.payload['url']
    content_type = document_response.payload['compressionAlgorithm']
    report_data = requests.get(download_url).content
    if content_type == 'GZIP':
        buf = io.BytesIO(report_data)
        with gzip.open(buf, 'rt') as f:
            return f.read()
    return report_data.decode('utf-8')


@app.route('/')
def home():
    return 'Middleware for Amazon Advertising API is running'


def request_and_download_report(report_type, marketplace, start_time, end_time, record_path):
    try:
        credentials = get_credentials()
    except ValueError as e:
        print(e)
        exit(1)  # Or handle more gracefully

    marketplace_str = marketplace.name  # Convert Marketplaces enum to string
    cached_report_id = get_cached_report_id(report_type, marketplace_str, start_time, end_time, record_path)
    if cached_report_id:
        #print(f"Using cached report ID: {cached_report_id}")
        report_id = cached_report_id
    else:
        reports_api = ReportsV2(credentials=credentials, marketplace=marketplace)
        report_id = request_report(reports_api, report_type, start_time, end_time)
        save_report_cache(report_type, marketplace_str, start_time, end_time, record_path, report_id)
        #print(f"Created new report ID: {report_id}")

    reports_api = ReportsV2(credentials=credentials, marketplace=marketplace)
    document_id = check_report_status(reports_api, report_id)
    if document_id:
        content = download_report(reports_api, document_id)
        data = json.loads(content)
        df = pd.json_normalize(data, record_path=record_path)
        json_data = json.loads(df.to_json(orient='records'))
        return json_data


@app.route('/get-sp-report', methods=['GET'])
def get_sp_report():
    report_type = request.args.get('reportType', 'GET_SALES_AND_TRAFFIC_REPORT')
    country_code = request.args.get('countryCode', 'US').upper()
    marketplace = marketplaces.get(country_code, Marketplaces.FR)  # Default to FR if not found

    try:
        start_time = parser.parse(request.args.get('startDate')) if 'startDate' in request.args else (
                datetime.utcnow() - timedelta(days=7))
        end_time = parser.parse(request.args.get('endDate')) if 'endDate' in request.args else datetime.utcnow()
    except ValueError as e:
        return jsonify({'status': 'error', 'message': 'Invalid date format. Please use YYYY-MM-DD format.'}), 400

    if start_time >= end_time:
        return jsonify({'status': 'error', 'message': 'The start date cannot be greater than the end date'}), 400

    record_path = request.args.get('recordPath', ['salesAndTrafficByAsin'])

    try:
        data = request_and_download_report(report_type, marketplace, start_time.isoformat(), end_time.isoformat(),
                                           record_path)
        return jsonify(data)
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=8000)
