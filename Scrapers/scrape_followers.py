import time
import requests
import pandas as pd
import random
import json
import datetime
import numpy as np
from UTILS.utils import wait_with_jitter

# Global variable to store active hours and the last update day
active_hours = []
last_update_day = None

def load_proxy_cookie_pairs(file_path):
    with open(file_path, 'r') as f:
        pairs = json.load(f)
    return [(pair['proxy'], pair['cookie']) for pair in pairs]

def get_all_following(user_id, proxy_cookie_pairs):
    base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
    params = {'count': 100}
    followers = []
    request_count = 0
    start_time = time.time()
    next_max_id = None

    # Load the last max_id if exists to resume from there
    try:
        with open('Files/last_max_id.txt', 'r') as file:
            next_max_id = file.read().strip()
            if next_max_id:
                params['max_id'] = next_max_id
    except FileNotFoundError:
        pass

    while True:
        wait_with_jitter()

        # Randomly select a (proxy, cookie) pair
        proxy, cookie = random.choice(proxy_cookie_pairs)
        print(f"Using proxy: {proxy}")

        try:
            # Parse proxy string
            proxy_parts = proxy.split(':')
            proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"

            response = requests.get(base_url, params=params, headers={
                'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
                'Accept-Language': 'en-US',
                'Accept-Encoding': 'gzip, deflate',
                'X-IG-Capabilities': '3brTvw==',
                'X-IG-Connection-Type': 'WIFI',
                'X-IG-App-ID': '567067343352427',
                'Cookie': cookie
            }, proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
            response.raise_for_status()
   
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            continue

        data = response.json()

        if 'next_max_id' in data:
            params['max_id'] = data['next_max_id']
            # Save the next_max_id to resume later if needed
            with open('last_max_id.txt', 'w') as file:
                file.write(params['max_id'])
        elif 'next_max_id' not in data:
            print("No next_max_id found, exiting loop.")
            break

        if not data.get('users'):
            print("No more users found, exiting loop.")
            break

        followers.extend(data['users'])

        # Save every 25 followers
        if len(followers) >= 25:
            followers_df = pd.DataFrame(followers)
            followers_df.to_csv('followers_list.csv', mode='a', header=False, index=False)
            followers = []  # Reset the list after saving

    # Save any remaining followers
    if followers:
        followers_df = pd.DataFrame(followers)
        followers_df.to_csv('followers_list.csv', mode='a', header=False, index=False)
        print("Saved remaining followers.")

# Main execution
if __name__ == "__main__":
    proxy_cookie_pairs = load_proxy_cookie_pairs('Files/proxy_cookie_pairs.json')
    user_id = "25922742395"
    get_all_following(user_id, proxy_cookie_pairs)