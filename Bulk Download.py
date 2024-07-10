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
        sleep(10)
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


def request_and_download_report(report_type, marketplace, start_time, end_time, record_path, save_path):
    try:
        credentials = get_credentials()
    except ValueError as e:
        print(e)
        exit(1)  # Or handle more gracefully

    marketplace_str = marketplace.name  # Convert Marketplaces enum to string
    cached_report_id = get_cached_report_id(report_type, marketplace_str, start_time, end_time, record_path)
    if cached_report_id:
        print(f"Using cached report ID: {cached_report_id}")
        report_id = cached_report_id
    else:
        reports_api = ReportsV2(credentials=credentials, marketplace=marketplace)
        report_id = request_report(reports_api, report_type, start_time, end_time)
        save_report_cache(report_type, marketplace_str, start_time, end_time, record_path, report_id)
        print(f"Created new report ID: {report_id}")

    reports_api = ReportsV2(credentials=credentials, marketplace=marketplace)
    document_id = check_report_status(reports_api, report_id)
    if document_id:
        content = download_report(reports_api, document_id)
        data = json.loads(content)
        df = pd.json_normalize(data, record_path=record_path)
        df.to_csv(save_path, index=False)  # Save the data to CSV file
        print(f"Report saved to {save_path}")


@app.route('/')
def home():
    return 'Middleware for Amazon Advertising API is running'


@app.route('/get-monthly-reports', methods=['GET'])
def get_monthly_reports():
    report_type = request.args.get('reportType', 'GET_SALES_AND_TRAFFIC_REPORT')
    country_code = request.args.get('countryCode', 'FR').upper()
    marketplace = marketplaces.get(country_code, Marketplaces.FR)  # Default to FR if not found

    start_date = datetime(2024, 2, 1)  # Starting date: June 2021
    end_date = datetime(2024, 2, 29)  # Ending date: June 2024
    current_date = start_date

    while current_date <= end_date:
        next_date = current_date + timedelta(days=30)  # Assuming roughly 30 days in a month
        start_time = current_date.isoformat()
        end_time = next_date.isoformat()

        year_month = current_date.strftime('%Y_%m')
        save_path = f'reports/{year_month}.csv'  # Save path with format Year/Month
        print("Time range:{0} & {1}".format(start_time, end_time))
        record_path = request.args.get('recordPath', ['salesAndTrafficByAsin'])

        try:
            request_and_download_report(report_type, marketplace, start_time, end_time, record_path, save_path)
        except Exception as e:
            print(f"Error fetching report for {year_month}: {e}")

        current_date = next_date

    return jsonify({'status': 'success', 'message': 'Monthly reports have been generated and saved.'})


if __name__ == '__main__':
    if not os.path.exists('reports'):
        os.makedirs('reports')
    app.run(debug=True, port=8000)
