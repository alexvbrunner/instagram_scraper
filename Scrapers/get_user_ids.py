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

# Global variables
MIN_REQUEST_INTERVAL = 1
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
MAX_WORKERS = 10

def get_database_connection():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='main',
            user='root',
            password='password'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        sys.exit(1)

def get_proxies_from_database(connection):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("""
            SELECT proxy_address, proxy_port, proxy_username, proxy_password
            FROM accounts 
            WHERE proxy_address IS NOT NULL
        """)
        proxies = cursor.fetchall()
        cursor.close()
        return proxies
    except Error as e:
        print(f"Error fetching proxies from database: {e}")
        sys.exit(1)

def extract_username(input_string):
    parsed = urlparse(input_string)
    if parsed.netloc in ['www.instagram.com', 'instagram.com']:
        return parsed.path.strip('/').split('/')[0]
    else:
        return input_string.strip()

def wait_with_jitter():
    jitter = np.random.uniform(MIN_REQUEST_INTERVAL, MIN_REQUEST_INTERVAL * 2)
    time.sleep(jitter)

def get_user_id(username, proxy, max_retries=3):
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    
    headers = {
        'User-Agent': USER_AGENT,
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-IG-App-ID': '936619743392459'
    }

    proxy_url = f"http://{proxy['proxy_username']}:{proxy['proxy_password']}@{proxy['proxy_address']}:{proxy['proxy_port']}"
    proxies = {'http': proxy_url, 'https': proxy_url}

    for attempt in range(max_retries):
        wait_with_jitter()
        
        print(f"Attempt {attempt + 1} for {username}")
        print(f"Using proxy: {proxy['proxy_address']}:{proxy['proxy_port']}")

        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)
            print(f"Response status code: {response.status_code}")

            if response.status_code in [400, 401, 403, 429]:
                print(f"Request failed ({response.status_code}) for {username}. Proxy might be blocked.")
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

def process_username(args):
    input_string, proxy = args
    username = extract_username(input_string)
    user_id = get_user_id(username, proxy)
    return input_string, username, user_id

def process_usernames(input_file, output_file, proxies):
    with open(input_file, 'r') as f:
        inputs = [line.strip() for line in f if line.strip()]
    
    total_inputs = len(inputs)
    processed_inputs = set()
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while len(processed_inputs) < total_inputs:
            remaining_inputs = [input_string for input_string in inputs if input_string not in processed_inputs]
            batch = remaining_inputs[:MAX_WORKERS]
            
            futures = [executor.submit(process_username, (input_string, random.choice(proxies))) for input_string in batch]
            
            for future in concurrent.futures.as_completed(futures):
                input_string, username, user_id = future.result()
                processed_inputs.add(input_string)
                results.append((input_string, username, user_id))
                print(f"Processed {len(processed_inputs)}/{total_inputs}")

    # Sort results to maintain original order
    results.sort(key=lambda x: inputs.index(x[0]))

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Input', 'Username', 'User ID'])
        for input_string, username, user_id in results:
            if user_id:
                writer.writerow([input_string, username, user_id])
                print(f"Found user ID for {username}: {user_id}")
            else:
                writer.writerow([input_string, username, ''])
                print(f"Could not find user ID for {username}")
    
    print(f"Results saved to {output_file}")

def main():
    connection = get_database_connection()
    proxies = get_proxies_from_database(connection)
    connection.close()

    if not proxies:
        print("No proxies found in the database.")
        sys.exit(1)

    print(f"Found {len(proxies)} proxies in the database.")

    input_file = 'input_usernames.txt'
    output_file = 'Files/user_ids.csv'

    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' not found.")
        sys.exit(1)

    process_usernames(input_file, output_file, proxies)

if __name__ == "__main__":
    main()