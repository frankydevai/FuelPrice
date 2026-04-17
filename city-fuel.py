import requests
import pandas as pd
from datetime import datetime
import time
import os

def send_to_telegram(filename):
    # Fetch Telegram credentials from Railway variables
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not bot_token or not chat_id:
        print("Telegram credentials missing! Please check your Railway variables.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    
    print(f"Sending {filename} to Telegram...")
    with open(filename, 'rb') as file:
        response = requests.post(url, data={'chat_id': chat_id}, files={'document': file})
        
    if response.status_code == 200:
        print("Successfully sent to Telegram!")
    else:
        print(f"Failed to send to Telegram. Error: {response.text}")

def extract_and_save():
    # Fetch login credentials from Railway variables
    user_email = os.getenv("LOGIN_EMAIL")
    user_password = os.getenv("LOGIN_PASSWORD")

    if not user_email or not user_password:
        print("Login credentials missing! Please check your Railway variables.")
        return

    login_payload = {
        "email": user_email, 
        "password": user_password
    }

    login_url = "https://auth.production.united-fuel.com/api/auth/login"
    data_url = "https://cmp-backend.production.united-fuel.com/api/v1/cabinet/station"

    base_headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Origin": "https://cabinet.united-fuel.com",
        "Referer": "https://cabinet.united-fuel.com/"
    }

    print("Logging in to get a fresh token...")

    login_response = requests.post(login_url, json=login_payload, headers=base_headers)
    
    if login_response.status_code not in [200, 201]:
        print("Login failed! Stopping script.")
        print("Response:", login_response.text)
        return 

    fresh_token = login_response.json().get('data', {}).get('accessToken')
    if not fresh_token:
        print("Could not find the access token in the response.")
        return

    print("Successfully logged in! Starting data extraction...")

    auth_headers = base_headers.copy()
    auth_headers["Authorization"] = f"Bearer {fresh_token}"

    all_stations_data = []
    page = 1
    limit = 100 
    total_pages = 1  

    while page <= total_pages:
        url = f"{data_url}?page={page}&limit={limit}"
        response = requests.get(url, headers=auth_headers)
        
        if response.status_code == 200:
            response_data = response.json()
            if page == 1:
                total_pages = response_data.get('pagination', {}).get('totalPages', 1)
            
            print(f" -> Fetching page {page} of {total_pages}...")
            all_stations_data.extend(response_data.get('data', []))
            page += 1
            time.sleep(1)
        else:
            print(f"Failed to fetch data on page {page}.")
            break

    if all_stations_data:
        df = pd.json_normalize(all_stations_data)
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
        filename = f"fuel_stations_{date_str}.csv"
        df.to_csv(filename, index=False)
        print(f"\nSuccess! Saved {len(df)} total rows to {filename}")
        
        send_to_telegram(filename)
    else:
        print("\nNo data was extracted.")

if __name__ == "__main__":
    extract_and_save()
