import requests
import pandas as pd
from datetime import datetime
import time
import os
import ast

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


# ==========================================
# --- CLEANING LOGIC (NO EXTERNAL FILES) ---
# ==========================================

def extract_from_json_list(val, key):
    """Helper function to extract specific fields from the messy stringified lists in the CSV."""
    try:
        data = ast.literal_eval(str(val))
        if isinstance(data, list) and len(data) > 0:
            return data[0].get(key)
    except:
        return None
    return None

def clean_file(input_filename):
    print(f"Cleaning formatting for {input_filename}...")
    
    # Load the raw file your code just generated
    df_base = pd.read_csv(input_filename)
    
    # Extract only the exact columns we need from the nested JSON strings
    df_base['Station'] = df_base['nameInEfs'].fillna(df_base.get('nameInFile', ''))
    df_base['Address'] = df_base['addresses'].apply(lambda x: extract_from_json_list(x, 'street'))
    df_base['City'] = df_base['addresses'].apply(lambda x: extract_from_json_list(x, 'city'))
    df_base['State'] = df_base['addresses'].apply(lambda x: extract_from_json_list(x, 'state'))
    df_base['longitude'] = df_base['addresses'].apply(lambda x: extract_from_json_list(x, 'longitude'))
    df_base['latitude'] = df_base['addresses'].apply(lambda x: extract_from_json_list(x, 'latitude'))
    df_base['Retail price'] = pd.to_numeric(df_base['fuelPrices'].apply(lambda x: extract_from_json_list(x, 'retailPrice')), errors='coerce')
    df_base['Discounted price'] = pd.to_numeric(df_base['fuelPrices'].apply(lambda x: extract_from_json_list(x, 'discountedPrice')), errors='coerce')

    # Format the final columns
    final_cols = ['Station', 'Address', 'City', 'State', 'longitude', 'latitude', 'Retail price', 'Discounted price']
    df_final = df_base[final_cols].copy()

    # Drop any remaining rows that have absolutely no location data from the API
    missing_condition = (
        pd.isna(df_final['Address']) | 
        (df_final['Address'].astype(str).str.lower() == 'none') | 
        (df_final['Address'].astype(str).str.strip() == '') |
        pd.isna(df_final['latitude']) | 
        pd.isna(df_final['longitude'])
    )

    df_complete = df_final[~missing_condition].copy()

    # Save cleaned file
    cleaned_filename = f"fully_cleaned_{input_filename}"
    df_complete.to_csv(cleaned_filename, index=False)
    
    print(f"Cleanup finished! Cleaned file saved as {cleaned_filename}")
    return cleaned_filename

# ==========================================
# --------- END CLEANING LOGIC -------------
# ==========================================


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
        # 1. Save the raw file
        df = pd.json_normalize(all_stations_data)
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
        raw_filename = f"fuel_stations_{date_str}.csv"
        df.to_csv(raw_filename, index=False)
        print(f"\nSuccess! Saved {len(df)} total raw rows to {raw_filename}")
        
        # 2. RUN THE CLEANING FUNCTION on the raw file
        cleaned_filename = clean_file(raw_filename)
        
        # 3. Send the CLEANED file to Telegram
        send_to_telegram(cleaned_filename)
    else:
        print("\nNo data was extracted.")

if __name__ == "__main__":
    extract_and_save()
