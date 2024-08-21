"""
Instagram User ID Scraper

This script fetches user IDs for Instagram usernames or profile URLs. It uses a single random cookie
from the database to make requests to Instagram's API, avoiding rate limiting and IP blocks.

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

# Global variables
MIN_REQUEST_INTERVAL = 1
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def load_random_cookie(file_path):
    with open(file_path, 'r') as f:
        pairs = json.load(f)
    random_pair = random.choice(pairs)
    print(f"Using cookie for account: {random_pair['name']}")
    return random_pair['cookie']

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

def get_user_id(username, cookie, max_retries=3):
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-IG-App-ID': '936619743392459',
        'Cookie': cookie
    }

    for attempt in range(max_retries):
        wait_with_jitter()
        
        print(f"Attempt {attempt + 1} for {username}")
        print(f"Using cookie: {cookie[:50]}...")  # Print first 50 chars of cookie

        try:
            response = requests.get(url, headers=headers, timeout=15)
            print(f"Response status code: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response content: {response.text[:200]}...")  # Print first 200 chars of response

            if response.status_code == 400 or response.status_code == 401:
                print(f"Bad Request ({response.status_code}) for {username}. Cookie might be invalid.")
                return None
            
            response.raise_for_status()
            data = response.json()
            return data['data']['user']['id']
        except requests.exceptions.RequestException as e:
            print(f"Request exception for {username}: {e}")
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {username}: {e}")
        except KeyError as e:
            print(f"KeyError in response for {username}: {e}")
        except Exception as e:
            print(f"Unexpected error for {username}: {e}")
        
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Max retries reached for {username}")
    
    return None

def process_usernames(input_file, output_file, cookie):
    with open(input_file, 'r') as f:
        inputs = [line.strip() for line in f if line.strip()]
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Input', 'Username', 'User ID'])
        
        for input_string in inputs:
            username = extract_username(input_string)
            user_id = get_user_id(username, cookie)
            
            if user_id:
                writer.writerow([input_string, username, user_id])
                print(f"Found user ID for {username}: {user_id}")
            else:
                writer.writerow([input_string, username, ''])
                print(f"Could not find user ID for {username}")
            
            f.flush()
    
    print(f"Results saved to {output_file}")

def main():
    cookie = load_random_cookie('Files/proxy_cookie_pairs.json')
    process_usernames('input_usernames.txt', 'Files/user_ids.csv', cookie)

if __name__ == "__main__":
    main()