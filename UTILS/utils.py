import requests 
import time
import datetime
import random
import numpy as np

# Global variables for active hours and last update day
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

def read_last_max_id():
    try:
        with open('Files/maxid/last_max_id.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def save_last_max_id(max_id):
    with open('Files/maxid/last_max_id.txt', 'w') as file:
        file.write(max_id)

def make_request(base_url, params, cookies, cookie_index):
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 105.0.0.11.118 (iPhone11,8; iOS 12_3_1; en_US; en-US; scale=2.00; 828x1792; 165586599)',
        'Cookie': cookies[cookie_index]
    }
    response = requests.get(base_url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def handle_rate_limiting(start_time, request_count):
    if request_count >= 3:
        elapsed_time = time.time() - start_time
        if elapsed_time < 60:
            time.sleep(60 - elapsed_time)
        return time.time(), 0
    return start_time, request_count