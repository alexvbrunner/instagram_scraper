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

def get_all_following(user_id, cookies):
    base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
    params = {'count': 100}
    followers = []
    request_count = 0
    start_time = time.time()
    next_max_id = None
    cookie_index = 0  # Index to track the current cookie being used

    # Load the last max_id if exists to resume from there
    try:
        with open('last_max_id.txt', 'r') as file:
            next_max_id = file.read().strip()
            if next_max_id:
                params['max_id'] = next_max_id
    except FileNotFoundError:
        pass

    while True:
        wait_with_jitter()

        try:
            response = requests.get(base_url, params=params, headers={
                'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 105.0.0.11.118 (iPhone11,8; iOS 12_3_1; en_US; en-US; scale=2.00; 828x1792; 165586599)',
                'Cookie': cookies[cookie_index]  # Use the current cookie
            })
            response.raise_for_status()
   
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [400, 429]:
                print(f"Error {e.response.status_code}: Switching cookies.")
                cookie_index = (cookie_index + 1) % len(cookies)  # Rotate to the next cookie
                if cookie_index == 0:  # If all cookies have been tried, wait longer
                    print("All cookies exhausted, waiting longer...")
                    time.sleep(300)  # Wait for 5 minutes
                continue
            else:
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

        # Save every 100 followers
        if len(followers) >= 25:
            followers_df = pd.DataFrame(followers)
            followers_df.to_csv('followers_list.csv', mode='a', header=False, index=False)
            followers = []  # Reset the list after saving

    # Save any remaining followers
    if followers:
        followers_df = pd.DataFrame(followers)
        followers_df.to_csv('followers_list.csv', mode='a', header=False, index=False)
        print("Saved remaining followers.")

# Example usage with multiple cookies
cookies = [
'mid=ZpZeDQAEAAHuc0IRe4KVoPgzAdLR; ig_did=36D34B39-BE6C-47C3-A307-52C8E408F81E; datr=OF6WZrvsarLn4bMSQpzdcNY7; shbid="1280\0547943566320\0541754224106:01f76f470a0474fbe7a0882e925071bcf8452ca7209139ee40bdb7fd08a9ddec40931021"; shbts="1722688106\0547943566320\0541754224106:01f72cb78207bb65e7b518f7e9724f3761d23c065e2500878da8f95ef4077759919a5f03"; csrftoken=Ya2bWZTCamRniAb3mLnLqX3lQZIooYII; ds_user_id=7943566320; sessionid=7943566320%3AgPttqfmrEoTzP9%3A10%3AAYeA3dBZktfFOb10-au2wMRwOPaGPS-k19M9mPbATQ; rur="LDC\0547943566320\0541754334913:01f743d632ec2f4605f6422642275bf617a6a95a31465abed49ad90a608f1d04d5a0b28e"; wd=459x812'
]
user_id="25922742395"
followers_list = get_all_following(user_id, cookies)