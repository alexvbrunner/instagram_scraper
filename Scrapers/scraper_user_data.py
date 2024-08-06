import requests
import json
import gender_guesser.detector as gender
import csv
from itertools import cycle
import time
import datetime
import random
import numpy as np
from UTILS.utils import wait_with_jitter
from UTILS.json_parsing import upload_to_database, parse_user_info

def get_user_info(user_id, cookies_string, proxy):
    # Convert cookies string to dictionary
    cookies = dict(cookie.split('=', 1) for cookie in cookies_string.split('; '))

    url = f"https://i.instagram.com/api/v1/users/{user_id}/info/"
    
    # Updated mobile user agent (Instagram v275.0.0.27.98)
    headers = {
        'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
        'Accept-Language': 'en-US',
        'Accept-Encoding': 'gzip, deflate',
        'X-IG-Capabilities': '3brTvw==',
        'X-IG-Connection-Type': 'WIFI',
        'X-IG-App-ID': '567067343352427',
    }
    
    # Parse proxy string
    proxy_parts = proxy.split(':')
    proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
    
    try:
        response = requests.get(url, cookies=cookies, headers=headers, proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
        
        # Calculate response size
        response_size = len(response.content)
        print(f"Response size: {response_size} bytes")
        
        # Calculate headers size
        headers_size = len('\r\n'.join(f'{k}: {v}' for k, v in response.headers.items()))
        print(f"Headers size: {headers_size} bytes")
        
        # Calculate total size
        total_size = response_size + headers_size
        print(f"Total size: {total_size} bytes")
                
        return response.json(), total_size
    except requests.RequestException as e:
        print(f"Error occurred: {e}")
        return None, 0

def guess_gender(name):
    d = gender.Detector()
    first_name = name.split()[0]
    return d.get_gender(first_name)

def load_proxies(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_user_ids(file_path):
    with open(file_path, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip header
        return [row[2] for row in csv_reader if row]

def load_proxy_cookie_pairs(file_path):
    with open(file_path, 'r') as f:
        pairs = json.load(f)
    return [(pair['proxy'], pair['cookie']) for pair in pairs]

def main():
    proxy_cookie_pairs = load_proxy_cookie_pairs('Files/proxy_cookie_pairs.json')
    user_ids = load_user_ids('Files/account_users.csv')

    for user_id in user_ids:
        wait_with_jitter()  # Add cooldown before each request
        
        # Randomly select a (proxy, cookie) pair
        proxy, cookie = random.choice(proxy_cookie_pairs)
        print(f"Processing user ID: {user_id} with proxy: {proxy}")
        
        user_info, bandwidth_used = get_user_info(user_id, cookie, proxy)

        if user_info and 'user' in user_info:
            parsed_data = parse_user_info(user_info)
            
            print("\nMain User Information:")
            print(f"Username: {parsed_data['username']}")
            print(f"Full Name: {parsed_data['full_name']}")
            print(f"Follower Count: {parsed_data['follower_count']}")
            print(f"Following Count: {parsed_data['following_count']}")
            print(f"Media Count: {parsed_data['media_count']}")
            print(f"Is Private: {parsed_data['is_private']}")
            print(f"Is Verified: {parsed_data['is_verified']}")
            print(f"Biography: {parsed_data['biography']}")
            
            print(f"\nTotal bandwidth used: {bandwidth_used} bytes")

            # Add gender guessing
            full_name = parsed_data['full_name']
            guessed_gender = guess_gender(full_name)
            print(f"Guessed gender for {full_name}: {guessed_gender}")
            
            # Add guessed gender to parsed_data
            parsed_data['gender'] = guessed_gender
            
            # Upload parsed data to the database
            upload_to_database(parsed_data)
            
        else:
            print(f"Failed to retrieve information for user ID: {user_id}")
        
        print("\n" + "="*50 + "\n")

if __name__ == "__main__":
    main()