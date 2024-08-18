"""
Instagram User ID Scraper

This script fetches user IDs for Instagram usernames or profile URLs. It can use a list of proxy and cookie pairs
to make requests to Instagram's API, avoiding rate limiting and IP blocks, or operate without proxies.

The script reads usernames or URLs from an input file, processes them to extract usernames, fetches the
corresponding user IDs, and saves the results to a CSV file.

Usage:
    Ensure the following files are present:
    - 'Files/proxy_cookie_pairs.json': JSON file containing proxy and cookie pairs
    - 'input_usernames.txt': Text file with Instagram usernames or profile URLs (one per line)

    Run the script to generate 'Files/user_ids.csv' with the results.

Note: This script requires the requests library to be installed.
"""

import config
import requests
import json
import random
import csv
from urllib.parse import urlparse
import sys
import time
import numpy as np
from requests import ConnectionError, Timeout, RequestException
from retrieve_cookies import retrieve_single_cookie


# Global variables
USE_PROXIES = False
MIN_REQUEST_INTERVAL = 1

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

class CookieState:
    def __init__(self, cookie, proxy, name, index):
        self.cookie = cookie
        self.proxy = proxy
        self.name = name
        self.index = index
        self.active = True
        self.last_request_time = 0
        self.fail_count = 0

def load_proxy_cookie_pairs(file_path):
    with open(file_path, 'r') as f:
        pairs = json.load(f)
    cookie_states = [CookieState(pair['cookie'], pair['proxy'], pair['name'], i) for i, pair in enumerate(pairs)]
    
    print("Loaded cookies:")
    for cs in cookie_states:
        masked_cookie = cs.cookie[:20] + "..." + cs.cookie[-20:]
        print(f"{cs.name}: {masked_cookie}")
    
    return cookie_states

def extract_username(input_string):
    parsed = urlparse(input_string)
    if parsed.netloc in ['www.instagram.com', 'instagram.com']:
        return parsed.path.strip('/').split('/')[0]
    else:
        return input_string.strip()

def wait_with_jitter():
    activity_type = random.choices(['quick', 'normal', 'engaged'], weights=[0.3, 0.5, 0.2])[0]
    
    if activity_type == 'quick':
        jitter = np.random.exponential(scale=2)
    elif activity_type == 'normal':
        jitter = np.random.normal(loc=10, scale=5)
    else:  # engaged
        jitter = np.random.normal(loc=30, scale=10)

    # Add micro-breaks
    if random.random() < 0.1:  # 10% chance of a micro-break
        jitter += np.random.uniform(60, 300)  # 1-5 minute break

    # Ensure minimum wait time
    jitter = max(jitter, MIN_REQUEST_INTERVAL)

    print(f"Waiting for {jitter:.2f} seconds.")
    time.sleep(jitter)

def get_user_id(username, cookie_state, cookie_states, max_retries=3):
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-IG-App-ID': '936619743392459',
    }

    for attempt in range(max_retries):
        wait_with_jitter()
        
        headers['Cookie'] = cookie_state.cookie
        print(f"Attempt {attempt + 1} for {username} with {cookie_state.name}")
        print(f"Using cookie: {cookie_state.cookie[:50]}...")  # Print first 50 chars of cookie

        try:
            response = requests.get(url, headers=headers, timeout=15)
            print(f"Response status code: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response content: {response.text[:200]}...")  # Print first 200 chars of response

            if response.status_code == 400 or response.status_code == 401:
                print(f"Bad Request ({response.status_code}) for {username} with {cookie_state.name}. Cookie might be invalid.")
                new_cookie = fetch_new_cookie(cookie_state.name)
                if new_cookie:
                    print(f"Successfully fetched new cookie for {cookie_state.name}")
                    cookie_state.cookie = new_cookie
                    cookie_state.fail_count = 0
                    continue
                else:
                    print(f"Failed to fetch new cookie for {cookie_state.name}. Marking as inactive.")
                    cookie_state.active = False
                    return None
            
            response.raise_for_status()
            data = response.json()
            cookie_state.last_request_time = time.time()
            cookie_state.fail_count = 0
            return data['data']['user']['id']
        except requests.exceptions.RequestException as e:
            print(f"Request exception for {username} with {cookie_state.name}: {e}")
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {username} with {cookie_state.name}: {e}")
        except KeyError as e:
            print(f"KeyError in response for {username} with {cookie_state.name}: {e}")
        except Exception as e:
            print(f"Unexpected error for {username} with {cookie_state.name}: {e}")
        
        cookie_state.fail_count += 1
        if cookie_state.fail_count >= 3:
            print(f"Cookie for {cookie_state.name} has failed 3 times. Fetching a new one.")
            new_cookie = fetch_new_cookie(cookie_state.name)
            if new_cookie:
                cookie_state.cookie = new_cookie
                cookie_state.fail_count = 0
                continue
            else:
                cookie_state.active = False
                return None
        
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Max retries reached for {username}")
    
    return None

def fetch_new_cookie(account_name):
    print(f"\n--- Attempting to fetch new cookie for {account_name} ---")
    try:
        with open('Files/instagram_accounts.json', 'r') as f:
            accounts = json.load(f)
        
        account = next((acc for acc in accounts if acc['username'] == account_name), None)
        
        if account:
            print(f"Account {account_name} found in instagram_accounts.json")
            
            new_cookie = retrieve_single_cookie(
                account['username'],
                account['password'],
                account.get('user_agent', USER_AGENT),
                account.get('totp_secret')
            )
            
            if new_cookie:
                print(f"New cookie retrieved for {account_name}")
                print(f"Cookie (first 50 chars): {new_cookie[:50]}...")
                return new_cookie
            else:
                print(f"Failed to retrieve new cookie for {account_name}")
        else:
            print(f"Account {account_name} not found in instagram_accounts.json")
    
    except Exception as e:
        print(f"Error fetching new cookie: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("--- Failed to retrieve new cookie ---")
    return None

def process_usernames(input_file, output_file, cookie_states):
    with open(input_file, 'r') as f:
        inputs = [line.strip() for line in f if line.strip()]
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Input', 'Username', 'User ID'])
        
        for input_string in inputs:
            username = extract_username(input_string)
            user_id = None
            
            # Shuffle the cookie_states list for each username
            random.shuffle(cookie_states)
            
            for cookie_state in cookie_states:
                if cookie_state.active:
                    user_id = get_user_id(username, cookie_state, cookie_states)
                    if user_id:
                        break
                    elif cookie_state.fail_count >= 3:
                        print(f"Cookie {cookie_state.name} failed. Trying next cookie.")
                    else:
                        # If it's not a complete failure, we'll stick with this cookie
                        break
            
            if user_id:
                writer.writerow([input_string, username, user_id])
                print(f"Found user ID for {username}: {user_id}")
            else:
                writer.writerow([input_string, username, ''])
                print(f"Could not find user ID for {username}")
            
            f.flush()
    
    print(f"Results saved to {output_file}")

def main():
    global USE_PROXIES
    
    USE_PROXIES = False
    print("Running without proxies.")

    cookie_states = load_proxy_cookie_pairs('Files/proxy_cookie_pairs.json')
    process_usernames('input_usernames.txt', 'Files/user_ids.csv', cookie_states)

if __name__ == "__main__":
    main()