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
    current_time = datetime.datetime.now()
    current_day = current_time.day

    # Update active hours with more variability
    if last_update_day != current_day:
        active_hours = [
            (random.randint(6, 9), random.randint(10, 12)),
            (random.randint(13, 16), random.randint(17, 19)),
            (random.randint(19, 21), random.randint(22, 23))
        ]
        last_update_day = current_day

    current_hour = current_time.hour
    current_minute = current_time.minute

    # Check if current time is within active hours
    if not any(start <= current_hour < end for start, end in active_hours):
        next_start = min((start for start, end in active_hours if start > current_hour), default=active_hours[0][0])
        mean_sleep_time = ((next_start - current_hour) % 24) * 3600 - current_minute * 60
        sleep_time = np.random.normal(loc=mean_sleep_time, scale=1800)  # Increased standard deviation
        print(f"Inactive hours, sleeping for {max(0, int(sleep_time))} seconds.")
        time.sleep(max(0, sleep_time))
        return

    # Simulate more human-like behavior during active hours
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


def get_all_following(user_id, cookies):
    base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
    params = {'count': 100}
    followers = []

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