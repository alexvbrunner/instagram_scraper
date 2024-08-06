"""
Instagram User ID Scraper

This script fetches user IDs for Instagram usernames or profile URLs. It uses a list of proxy and cookie pairs
to make requests to Instagram's API, avoiding rate limiting and IP blocks.

The script reads usernames or URLs from an input file, processes them to extract usernames, fetches the
corresponding user IDs, and saves the results to a CSV file.

Usage:
    Ensure the following files are present:
    - 'Files/proxy_cookie_pairs.json': JSON file containing proxy and cookie pairs
    - 'Files/input_usernames.txt': Text file with Instagram usernames or profile URLs (one per line)

    Run the script to generate 'Files/user_ids.csv' with the results.

Note: This script requires the requests library to be installed.
"""

import requests
import json
import random
import csv
from urllib.parse import urlparse

def load_proxy_cookie_pairs(file_path):
    with open(file_path, 'r') as f:
        pairs = json.load(f)
    return [(pair['proxy'], pair['cookie']) for pair in pairs]

def extract_username(input_string):
    # Check if it's a URL
    parsed = urlparse(input_string)
    if parsed.netloc in ['www.instagram.com', 'instagram.com']:
        # Extract username from path
        return parsed.path.strip('/').split('/')[0]
    else:
        # Assume it's already a username
        return input_string.strip()

def get_user_id(username, proxy, cookie):
    url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
    
    headers = {
        'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
        'Accept-Language': 'en-US',
        'Accept-Encoding': 'gzip, deflate',
        'X-IG-Capabilities': '3brTvw==',
        'X-IG-Connection-Type': 'WIFI',
        'X-IG-App-ID': '567067343352427',
        'Cookie': cookie
    }

    # Parse proxy string
    proxy_parts = proxy.split(':')
    proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"

    try:
        response = requests.get(url, headers=headers, proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data['data']['user']['id']
    except requests.RequestException as e:
        print(f"Error fetching user ID for {username}: {e}")
        return None

def process_usernames(input_file, output_file, proxy_cookie_pairs):
    results = []
    
    with open(input_file, 'r') as f:
        inputs = [line.strip() for line in f if line.strip()]
    
    for input_string in inputs:
        username = extract_username(input_string)
        proxy, cookie = random.choice(proxy_cookie_pairs)
        user_id = get_user_id(username, proxy, cookie)
        
        if user_id:
            results.append((input_string, username, user_id))
            print(f"Found user ID for {username}: {user_id}")
        else:
            print(f"Could not find user ID for {username}")
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Input', 'Username', 'User ID'])
        writer.writerows(results)
    
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    proxy_cookie_pairs = load_proxy_cookie_pairs('Files/proxy_cookie_pairs.json')
    process_usernames('Files/input_usernames.txt', 'Files/user_ids.csv', proxy_cookie_pairs)