"""
Instagram User ID Scraper

This script fetches user IDs for Instagram usernames or profile URLs. It uses a random proxy
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
import mysql.connector
from mysql.connector import Error
import concurrent.futures
import os
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

# Global variables
MIN_REQUEST_INTERVAL = 5
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
MAX_WORKERS = 10  # Changed from 50 to 10
MAX_RETRIES = 5
MAX_PROXY_FAILURES = 3

class ProxyManager:
    def __init__(self, proxies):
        self.proxies = proxies
        self.failure_count = defaultdict(int)
        self.blocked_proxies = set()

    def get_proxy(self):
        available_proxies = [p for p in self.proxies if self._proxy_id(p) not in self.blocked_proxies]
        if not available_proxies:
            raise Exception("No available proxies left")
        return random.choice(available_proxies)

    def mark_failure(self, proxy):
        proxy_id = self._proxy_id(proxy)
        self.failure_count[proxy_id] += 1
        if self.failure_count[proxy_id] >= MAX_PROXY_FAILURES:
            self.blocked_proxies.add(proxy_id)
            print(f"Proxy {proxy['proxy_address']}:{proxy['proxy_port']} has been blocked due to multiple failures")

    def get_active_proxy_count(self):
        return len(self.proxies) - len(self.blocked_proxies)

    def _proxy_id(self, proxy):
        return f"{proxy['proxy_address']}:{proxy['proxy_port']}"

def get_proxies_from_webshare():
    load_dotenv()
    api_key = os.getenv('WEBSHARE_API_KEY')
    if not api_key:
        print("WEBSHARE_API_KEY not found in environment variables.")
        sys.exit(1)

    url = "https://proxy.webshare.io/api/proxy/list/"
    headers = {
        "Authorization": f"Token {api_key}"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        proxies = []
        for proxy in data['results']:
            proxies.append({
                'proxy_address': proxy['proxy_address'],
                'proxy_port': proxy['ports']['http'],
                'proxy_username': proxy['username'],
                'proxy_password': proxy['password']
            })
        
        return proxies
    except requests.exceptions.RequestException as e:
        print(f"Error fetching proxies from Webshare: {e}")
        sys.exit(1)

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
    jitter = max(jitter, 1)

    print(f"Waiting for {jitter:.2f} seconds.")
    time.sleep(jitter)

def get_user_id(username, proxy_manager, max_retries=MAX_RETRIES):
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-IG-App-ID': '936619743392459'
    }

    for attempt in range(max_retries):
        wait_with_jitter()  # Use the new wait_with_jitter function
        
        proxy = proxy_manager.get_proxy()
        proxy_url = f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy_address']}:{proxy['proxy_port']}"
        proxies = {'http': proxy_url, 'https': proxy_url}

        print(f"Attempt {attempt + 1} for {username}")
        print(f"Using proxy: {proxy['proxy_address']}:{proxy['proxy_port']}")

        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            print(f"Response status code: {response.status_code}")

            if response.status_code in [400, 401, 403, 429]:
                print(f"Request failed ({response.status_code}) for {username}. Proxy might be blocked.")
                proxy_manager.mark_failure(proxy)
                continue

            response.raise_for_status()
            data = response.json()
            return data['data']['user']['id']
        except requests.exceptions.RequestException as e:
            print(f"Request exception for {username}: {e}")
            proxy_manager.mark_failure(proxy)
        except json.JSONDecodeError as e:
            print(f"JSON decode error for {username}: {e}")
            proxy_manager.mark_failure(proxy)
        except KeyError as e:
            print(f"KeyError in response for {username}: {e}")
            proxy_manager.mark_failure(proxy)
        except Exception as e:
            print(f"Unexpected error for {username}: {e}")
            proxy_manager.mark_failure(proxy)
        
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Max retries reached for {username}")
    
    return None

def process_usernames(input_file, output_file, proxies):
    # Read existing user IDs
    existing_data = {}
    if os.path.exists(output_file):
        with open(output_file, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header
            for row in reader:
                if len(row) >= 3:
                    existing_data[row[1]] = row[2]  # Username: User ID

    with open(input_file, 'r') as f:
        inputs = [line.strip() for line in f if line.strip()]
    
    total_inputs = len(inputs)
    processed_inputs = set()
    results = []

    start_time = time.time()
    proxy_manager = ProxyManager(proxies)
    
    with open(output_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if not existing_data:
            writer.writerow(['Input', 'Username', 'User ID'])

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while len(processed_inputs) < total_inputs:
                remaining_inputs = [input_string for input_string in inputs if input_string not in processed_inputs]
                batch = remaining_inputs[:MAX_WORKERS]
                
                futures = [executor.submit(process_username, (input_string, proxy_manager, existing_data)) for input_string in batch]
                
                for future in concurrent.futures.as_completed(futures):
                    input_string, username, user_id = future.result()
                    processed_inputs.add(input_string)
                    results.append((input_string, username, user_id))
                    
                    if user_id and user_id != 'existing':
                        writer.writerow([input_string, username, user_id])
                        csvfile.flush()
                    
                    processed_count = len(processed_inputs)
                    print(f"Processed {processed_count}/{total_inputs}")
                    print(f"Active proxies: {proxy_manager.get_active_proxy_count()}/{len(proxies)}")
                    
                    # Calculate and print estimated time of completion
                    elapsed_time = time.time() - start_time
                    avg_time_per_input = elapsed_time / processed_count
                    remaining_inputs = total_inputs - processed_count
                    estimated_time_remaining = avg_time_per_input * remaining_inputs
                    estimated_completion_time = datetime.now() + timedelta(seconds=estimated_time_remaining)
                    print(f"Estimated time of completion: {estimated_completion_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Final check for any missed users
    missed_users = [input_string for input_string in inputs if input_string not in processed_inputs]
    if missed_users:
        print(f"Found {len(missed_users)} missed users. Retrying...")
        for input_string in missed_users:
            username = extract_username(input_string)
            user_id = get_user_id(username, proxy_manager, max_retries=MAX_RETRIES)
            results.append((input_string, username, user_id))
            with open(output_file, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if user_id:
                    writer.writerow([input_string, username, user_id])
                else:
                    writer.writerow([input_string, username, ''])
        print("Finished processing missed users.")

    # Sort results to maintain original order
    results.sort(key=lambda x: inputs.index(x[0]))

    print(f"Results saved to {output_file}")

def process_username(args):
    input_string, proxy_manager, existing_data = args
    username = extract_username(input_string)
    
    if username in existing_data:
        return input_string, username, 'existing'
    
    user_id = get_user_id(username, proxy_manager)
    return input_string, username, user_id

def main():
    proxies = get_proxies_from_webshare()

    if not proxies:
        print("No proxies found from Webshare.")
        sys.exit(1)

    print(f"Found {len(proxies)} proxies from Webshare.")

    input_file = 'input_usernames.txt'
    output_file = 'Files/user_ids.csv'

    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        sys.exit(1)

    process_usernames(input_file, output_file, proxies)

if __name__ == "__main__":
    main()