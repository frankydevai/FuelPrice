import requests
import pandas as pd
from datetime import datetime
import time

def extract_and_save():
    # ==========================================
    # 1. YOUR LOGIN CREDENTIALS
    # ==========================================
    login_payload = {
        "email": "fleet@brrtransport.com", 
        "password": "123456"
    }

    # The exact URLs for logging in and getting the data
    login_url = "https://auth.production.united-fuel.com/api/auth/login"
    data_url = "https://cmp-backend.production.united-fuel.com/api/v1/cabinet/station"

    # Stealth headers
    base_headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Origin": "https://cabinet.united-fuel.com",
        "Referer": "https://cabinet.united-fuel.com/"
    }

    print("Logging in to get a fresh token...")

    # ==========================================
    # 2. AUTOMATIC LOGIN
    # ==========================================
    login_response = requests.post(login_url, json=login_payload, headers=base_headers)
    
    if login_response.status_code != 200 and login_response.status_code != 201:
        print("Login failed! Stopping script.")
        print("Error details:", login_response.text)
        return 

    token_data = login_response.json()
    fresh_token = token_data.get('data', {}).get('accessToken')
    
    if not fresh_token:
        print("Could not find the access token in the response.")
        return

    print("Successfully logged in! Starting data extraction...")

    auth_headers = base_headers.copy()
    auth_headers["Authorization"] = f"Bearer {fresh_token}"

    # ==========================================
    # 3. EXTRACTION LOOP
    # ==========================================
    all_stations_data = []
    page = 1
    limit = 100  # Extracting 100 at a time for maximum speed!
    total_pages = 1  

    while page <= total_pages:
        url = f"{data_url}?page={page}&limit={limit}"
        response = requests.get(url, headers=auth_headers)
        
        if response.status_code == 200:
            response_data = response.json()
            
            # Update the total pages dynamically on the first request
            if page == 1:
                total_pages = response_data.get('pagination', {}).get('totalPages', 1)
                total_items = response_data.get('pagination', {}).get('totalItems', 'Unknown')
                print(f"Found {total_items} total stations. Fetching {total_pages} pages...")
            
            print(f" -> Fetching page {page} of {total_pages}...")
            
            # Extract the actual stations data
            stations = response_data.get('data', [])
            all_stations_data.extend(stations)
            
            page += 1
            time.sleep(1) # Be polite to the server
            
        else:
            print(f"Failed to fetch data on page {page}. Status code: {response.status_code}")
            print(f"Server response: {response.text}")
            break

    # ==========================================
    # 4. SAVE TO CSV
    # ==========================================
    if all_stations_data:
        df = pd.json_normalize(all_stations_data)
        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"fuel_stations_{date_str}.csv"
        df.to_csv(filename, index=False)
        print(f"\nSuccess! Saved {len(df)} total rows to {filename}")
    else:
        print("\nNo data was extracted.")

if __name__ == "__main__":
    extract_and_save()