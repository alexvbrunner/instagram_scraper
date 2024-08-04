import time
import requests
import pandas as pd
from UTILS.utils import *
import datetime
import random
import numpy as np


active_hours = []
last_update_day = None


def wait_with_jitter():
    global active_hours, last_update_day
    current_day = datetime.datetime.now().day

    # Update active hours only if the day has changed
    if last_update_day != current_day:
        active_hours = [
            (random.randint(6, 8), random.randint(9, 11)),
            (random.randint(13, 15), random.randint(16, 18)),
            (random.randint(18, 20), random.randint(21, 23))
        ]
        last_update_day = current_day

    start_time = time.time()

    current_hour = datetime.datetime.now().hour

    if not any(start <= current_hour < end for start, end in active_hours):
        next_start = min((start for start, end in active_hours if start > current_hour), default=24)
        # More realistic variability in sleep time calculation using Gaussian distribution
        mean_sleep_time = ((next_start - current_hour) % 24) * 3600
        sleep_time = np.random.normal(loc=mean_sleep_time, scale=300)  # Standard deviation of 5 minutes
        sleep_time -= datetime.datetime.now().minute * 60 + datetime.datetime.now().second
        print(f"Inactive hours, sleeping for {max(0, sleep_time)} seconds.")
        print(active_hours)
        time.sleep(max(0, sleep_time))
        return

    elapsed_time = time.time() - start_time
    # Gaussian distribution for jitter to simulate more natural variance
    jitter = int(np.random.normal(loc=5, scale=5))  # Mean of 5 seconds, SD of 5 seconds
    required_sleep = (20 + jitter) - elapsed_time  # Base sleep of 15 seconds
    if required_sleep > 0:
        time.sleep(required_sleep)
    start_time = time.time()



def get_all_following(user_id, cookies):
    base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
    params = {'count': 100}
    followers = []
    request_count = 0

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
    'cookie_string_here'
]

user_id = "25922742395"
followers_list = get_all_following(user_id, cookies)