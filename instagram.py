import time
import requests
import pandas as pd
from UTILS.utils import *

def get_all_following(user_id, cookies):
    base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
    params = {'count': 100}
    followers = []
    request_count = 0
    start_time = time.time()
    next_max_id = read_last_max_id()
    if next_max_id:
        params['max_id'] = next_max_id
    cookie_index = 0

    while True:
        start_time, request_count = handle_rate_limiting(start_time, request_count)
        try:
            data = make_request(base_url, params, cookies, cookie_index)
            request_count += 1
        except requests.exceptions.HTTPError as e:
            if e.response.status_code in [400, 429]:
                print(f"Error {e.response.status_code}: Switching cookies.")
                cookie_index = (cookie_index + 1) % len(cookies)
                if cookie_index == 0:
                    print("All cookies exhausted, waiting longer...")
                    time.sleep(3600)
                continue
            else:
                print(f"Request failed: {e}")
                continue

        if 'next_max_id' in data:
            params['max_id'] = data['next_max_id']
            save_last_max_id(params['max_id'])
        elif 'next_max_id' not in data:
            print("No next_max_id found, exiting loop.")
            break

        if not data.get('users'):
            print("No more users found, exiting loop.")
            break

        followers.extend(data['users'])
        if len(followers) >= 25:
            pd.DataFrame(followers).to_csv('followers_list.csv', mode='a', header=False, index=False)
            followers = []

    if followers:
        pd.DataFrame(followers).to_csv('followers_list.csv', mode='a', header=False, index=False)
        print("Saved remaining followers.")

# Example usage with multiple cookies
cookies = [
    'cookie_string_here'
]

user_id = "25922742395"
followers_list = get_all_following(user_id, cookies)

