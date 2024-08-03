import requests 
import time

def read_last_max_id():
    try:
        with open('last_max_id.txt', 'r') as file:
            return file.read().strip()
    except FileNotFoundError:
        return None

def save_last_max_id(max_id):
    with open('last_max_id.txt', 'w') as file:
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