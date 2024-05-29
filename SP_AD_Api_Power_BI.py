import gzip
import io
import os
import sqlite3
import requests
from flask import Flask, redirect, request, jsonify, session
from urllib.parse import urlencode
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil import parser
from apscheduler.schedulers.background import BackgroundScheduler
from ad_api.base import AdvertisingApiException, Marketplaces, MarketplacesIds
from ad_api.api import Reports
import time
import json
import pandas as pd

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

CLIENT_ID = os.getenv('AD_API_CLIENT_ID')
CLIENT_SECRET = os.getenv('AD_API_CLIENT_SECRET')
REDIRECT_URI = "http://127.0.0.1:5000/amazonlogin"
SCOPE = "advertising::campaign_management"
AUTHORIZATION_URL = "https://eu.account.amazon.com/ap/oa"
TOKEN_URL = "https://api.amazon.co.uk/auth/o2/token"

# Mapping of country codes to Marketplaces
marketplaces = {
    'AE': Marketplaces.AE, 'BE': Marketplaces.BE, 'DE': Marketplaces.DE, 'PL': Marketplaces.PL,
    'EG': Marketplaces.EG, 'ES': Marketplaces.ES, 'FR': Marketplaces.FR, 'GB': Marketplaces.GB,
    'IN': Marketplaces.IN, 'IT': Marketplaces.IT, 'NL': Marketplaces.NL, 'SA': Marketplaces.SA,
    'SE': Marketplaces.SE, 'TR': Marketplaces.TR, 'UK': Marketplaces.UK,
    'AU': Marketplaces.AU, 'JP': Marketplaces.JP, 'SG': Marketplaces.SG, 'US': Marketplaces.US,
    'BR': Marketplaces.BR, 'CA': Marketplaces.CA, 'MX': Marketplaces.MX
}

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS profiles (
            profile_id TEXT PRIMARY KEY,
            account_id TEXT,
            marketplace_id TEXT,
            name TEXT,
            country_code TEXT,
            currency_code TEXT,
            daily_budget REAL,
            timezone TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS report_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id TEXT,
            start_date TEXT,
            end_date TEXT,
            report_type TEXT,
            time_unit TEXT,
            marketplace TEXT,
            report_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()

def save_tokens(access_token, refresh_token):
    print(f"Saving tokens: access_token={access_token}, refresh_token={refresh_token}")
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('DELETE FROM tokens')
    c.execute('INSERT INTO tokens (access_token, refresh_token) VALUES (?, ?)', (access_token, refresh_token))
    conn.commit()
    conn.close()

def get_tokens():
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('SELECT access_token, refresh_token FROM tokens')
    tokens = c.fetchone()
    conn.close()
    print(f"Retrieved tokens from DB: {tokens}")
    if tokens:
        return {'access_token': tokens[0], 'refresh_token': tokens[1]}
    return None

def get_access_token():
    tokens = get_tokens()
    if not tokens:
        raise ValueError("No refresh token found. Please authorize first.")
    refresh_token = tokens['refresh_token']
    token_data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(TOKEN_URL, data=token_data)
    if response.status_code == 200:
        tokens = response.json()
        print(f"Refreshed tokens: {tokens}")
        save_tokens(tokens['access_token'], tokens['refresh_token'])
        return tokens['access_token']
    else:
        raise ValueError(f"Failed to refresh token: {response.text}")

def get_credentials():
    access_token = get_access_token()
    return {
        'access_token': access_token,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'refresh_token': get_tokens()['refresh_token']
    }

def save_profiles(profiles):
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('DELETE FROM profiles')
    for profile in profiles:
        c.execute('''
            INSERT INTO profiles (profile_id, account_id, marketplace_id, name, country_code, currency_code, daily_budget, timezone)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            profile['profileId'],
            profile['accountInfo']['id'],
            profile['accountInfo']['marketplaceStringId'],
            profile['accountInfo']['name'],
            profile['countryCode'],
            profile['currencyCode'],
            profile['dailyBudget'],
            profile['timezone']
        ))
    conn.commit()
    conn.close()

def get_profiles_from_db():
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('SELECT * FROM profiles')
    profiles = c.fetchall()
    conn.close()
    return profiles

def save_report_cache(profile_id, start_date, end_date, report_type, time_unit, marketplace, report_id):
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('''
        INSERT INTO report_cache (profile_id, start_date, end_date, report_type, time_unit, marketplace, report_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (profile_id, start_date, end_date, report_type, time_unit, marketplace, report_id))
    conn.commit()
    conn.close()

def get_report_cache(profile_id, start_date, end_date, report_type, time_unit, marketplace):
    conn = sqlite3.connect('tokens.db')
    c = conn.cursor()
    c.execute('''
        SELECT report_id FROM report_cache
        WHERE profile_id = ? AND start_date = ? AND end_date = ? AND report_type = ? AND time_unit = ? AND marketplace = ?
        ORDER BY created_at DESC LIMIT 1
    ''', (profile_id, start_date, end_date, report_type, time_unit, marketplace))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None

@app.route('/')
def home():
    return 'Middleware for Amazon Advertising API is running'

@app.route('/authorize')
def authorize():
    params = {
        "client_id": CLIENT_ID,
        "scope": SCOPE,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI
    }
    url = f"{AUTHORIZATION_URL}?{urlencode(params)}"
    return redirect(url)

@app.route('/amazonlogin')
def callback():
    code = request.args.get('code')
    if code:
        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }
        response = requests.post(TOKEN_URL, data=token_data)
        if response.status_code == 200:
            tokens = response.json()
            print(f"Authorization tokens received: {tokens}")
            session['access_token'] = tokens['access_token']
            session['refresh_token'] = tokens['refresh_token']
            save_tokens(tokens['access_token'], tokens['refresh_token'])
            return 'Login successful, you can close this page now.'  # Display a message instead of redirecting
        else:
            return f"Failed to fetch tokens: {response.text}", 400
    return "Authorization failed", 400

@app.route('/get-profiles', methods=['GET'])
def get_profiles():
    try:
        credentials = get_credentials()
        headers = {
            'Authorization': f"Bearer {credentials['access_token']}",
            'Amazon-Advertising-API-ClientId': CLIENT_ID,
            'Content-Type': 'application/json'
        }
        response = requests.get('https://advertising-api-eu.amazon.com/v2/profiles', headers=headers)
        if response.status_code == 200:
            profiles = response.json()
            save_profiles(profiles)
            return jsonify(profiles)
        else:
            return jsonify({'status': 'error', 'message': response.text}), response.status_code
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

def request_and_download_report(profile_id, start_date, end_date, marketplace, report_type="spAdvertisedProduct",
                                time_unit="SUMMARY"):
    try:
        credentials = get_credentials()
    except ValueError as e:
        print(e)
        exit(1)  # Or handle more gracefully

    print(f"Using credentials: {credentials}")
    print(f"Requesting report for profile_id={profile_id}, start_date={start_date}, end_date={end_date}, marketplace={marketplace}, report_type={report_type}, time_unit={time_unit}")

    # Ensure the date range is within the last 90 days
    today = datetime.utcnow()
    start_date_obj = parser.parse(start_date)
    end_date_obj = parser.parse(end_date)

    if report_type == "spAdvertisedProduct":
        if (today - start_date_obj).days > 90 or (today - end_date_obj).days > 90:
            raise ValueError("Date range exceeds the maximum retention period of 90 days.")
        if time_unit == "SUMMARY":
            columns = ["startDate", "endDate", "impressions", "clicks", "cost", "advertisedAsin",
                       "unitsSoldSameSku7d", "unitsSoldOtherSku7d",
                       "sales7d", "advertisedSku"]
        else:
            columns = ["date", "impressions", "clicks", "cost", "advertisedAsin",
                       "unitsSoldSameSku7d", "unitsSoldOtherSku7d",
                       "sales7d", "advertisedSku"]
    elif report_type == "sdAdvertisedProduct":
        if (today - start_date_obj).days > 65 or (today - end_date_obj).days > 65:
            raise ValueError("Date range exceeds the maximum retention period of 65 days.")
        if time_unit == "SUMMARY":
            columns = [
                "startDate", "endDate", "promotedSku", "promotedAsin", "impressions", "clicks", "cost", "unitsSold",
                "sales",
            ]
        else:
            columns = [
                "date", "promotedSku", "promotedAsin", "impressions", "clicks", "cost", "unitsSold",
                "sales",
            ]
    else:
        raise ValueError("Unsupported report type")

    # Check if the same request was made before and retrieve the report ID if it exists
    cached_report_id = get_report_cache(profile_id, start_date, end_date, report_type, time_unit, marketplace.name)
    if cached_report_id:
        print(f"Using cached report ID: {cached_report_id}")
        report_id = cached_report_id
        reports = Reports(
            marketplace=marketplace,
            credentials={
                'refresh_token': credentials['refresh_token'],
                'client_id': credentials['client_id'],
                'client_secret': credentials['client_secret'],
                'profile_id': str(profile_id)  # Convert profile_id to string
            },
            access_token=credentials['access_token']
        )
    else:
        reports = Reports(
            marketplace=marketplace,
            credentials={
                'refresh_token': credentials['refresh_token'],
                'client_id': credentials['client_id'],
                'client_secret': credentials['client_secret'],
                'profile_id': str(profile_id)  # Convert profile_id to string
            },
            access_token=credentials['access_token']
        )

        report_body = {
            "name": "report_name",
            "startDate": start_date,
            "endDate": end_date,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS" if report_type == "spAdvertisedProduct" else "SPONSORED_DISPLAY",
                "columns": columns,
                "reportTypeId": report_type,
                "format": "GZIP_JSON",
                "groupBy": ["advertiser"],
                "timeUnit": time_unit
            }
        }

        report = reports.post_report(body=report_body)
        report_id = report.payload['reportId']
        save_report_cache(profile_id, start_date, end_date, report_type, time_unit, marketplace.name, report_id)
        print(f"Created new report ID: {report_id}")

    # Poll the report status until it is available
    while True:
        report_status = reports.get_report(reportId=report_id)
        if report_status.payload['status'] == 'COMPLETED':
            print("REPORT STATUS: COMPLETED")
            break
        elif report_status.payload['status'] == 'FAILED':
            print("REPORT STATUS: FAILED")
            return jsonify({'status': 'error', 'message': 'Failed to generate report'}), 500
        time.sleep(10)  # Wait for 10 seconds before checking the status again

    # Download the report
    download_url = report_status.payload['url']
    report_data = requests.get(download_url).content
    buf = io.BytesIO(report_data)
    with gzip.open(buf, 'rt') as f:
        report_content = f.read()
    report_json = json.loads(report_content)
    df = pd.json_normalize(report_json)
    json_data = json.loads(df.to_json(orient='records'))
    return json_data

@app.route('/get-ad-report', methods=['GET'])
def get_ad_report():
    try:
        report_type = request.args.get('reportType')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        time_unit = request.args.get('timeUnit', 'SUMMARY')  # Default to SUMMARY if not provided
        marketplace_str = request.args.get('marketplace')  # No default value
        print(marketplace_str)
        profile_name = request.args.get('profileName')  # Profile name to filter by
        profile_name = profile_name.replace("%20"," ")
        print(profile_name)
        marketplace = marketplaces.get(marketplace_str)

        if not report_type or not start_date or not end_date or not profile_name or not marketplace:
            return jsonify({'status': 'error', 'message': 'Missing required parameters'}), 400

        # Retrieve profiles from database
        profiles = get_profiles_from_db()
        profile_id = None
        for profile in profiles:
            if profile[3] == profile_name and profile[4] == marketplace_str:
                profile_id = profile[0]
                print(profile_id)
                break

        if not profile_id:
            return jsonify({'status': 'error', 'message': 'Profile ID not found for the specified profile name and marketplace'}), 400

        # Parse dates
        start_date = parser.parse(start_date).strftime('%Y-%m-%d')
        end_date = parser.parse(end_date).strftime('%Y-%m-%d')

        # Request and download the report
        report_data = request_and_download_report(profile_id, start_date, end_date, marketplace, report_type, time_unit)
        return jsonify(report_data)
    except AdvertisingApiException as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

def refresh_access_token():
    try:
        get_access_token()
        print("Access token refreshed successfully")
    except Exception as e:
        print(f"Failed to refresh access token: {str(e)}")

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(refresh_access_token, 'interval', minutes=55)  # Refresh token every 55 minutes
    scheduler.start()

    try:
        app.run(debug=True, port=5000)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
