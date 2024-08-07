import requests
import pandas as pd
from datetime import datetime
import random
import logging
import time
import threading
import numpy as np
import os
import json
import concurrent.futures

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CookieState:
    def __init__(self, cookie, proxy, name, index):
        self.cookie = cookie
        self.proxy = proxy
        self.name = name
        self.index = index
        self.active = True
        self.last_request_time = 0
        self.fail_count = 0

class InstagramScraper:
    def __init__(self, user_id):
        self.user_id = user_id
        self.cookie_states = self.load_proxy_cookie_pairs()
        self.base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
        self.params = {'count': 25}
        self.followers = []
        self.cookie_state_lock = threading.Lock()
        self.max_id_lock = threading.Lock()
        self.min_request_interval = 1
        self.total_followers_scraped = 0
        self.base_encoded_part = None
        self.large_step = 250
        self.small_step = 25
        self.global_iteration = 0
        self.last_max_id = "0|"
        self.current_cookie_index = 0
        self.max_retries = 3
        self.stop_event = threading.Event()
        self.max_workers = len(self.cookie_states)

    def load_proxy_cookie_pairs(self):
        with open('Files/proxy_cookie_pairs.json', 'r') as f:
            pairs = json.load(f)
        return [CookieState(pair['cookie'], pair['proxy'], pair['name'], i) for i, pair in enumerate(pairs)]

    def load_state(self):
        if os.path.exists('scraper_state.json'):
            with open('scraper_state.json', 'r') as f:
                state = json.load(f)
            self.user_id = state['user_id']
            self.cookie_states = []
            for cs in state['cookie_states']:
                cookie_state = CookieState(
                    cs['cookie'],
                    cs.get('proxy', ''),  # Use an empty string if 'proxy' is not present
                    cs.get('name', f"Cookie {cs['index']}"),  # Use a default name if 'name' is not present
                    cs['index']
                )
                cookie_state.active = cs['active']
                cookie_state.last_request_time = cs['last_request_time']
                cookie_state.fail_count = cs.get('fail_count', 0)
                self.cookie_states.append(cookie_state)
            self.total_followers_scraped = state['followers_count']
            self.base_encoded_part = state.get('base_encoded_part')
            self.global_iteration = state.get('global_iteration', 0)
            self.last_max_id = state.get('last_max_id', "0|")
            self.current_cookie_index = state.get('current_cookie_index', 0)
            print(f"Resumed scraping. Previously scraped {self.total_followers_scraped} followers.")
            print(f"Global iteration: {self.global_iteration}, Last max_id: {self.last_max_id}")
        else:
            print("No previous state found. Starting fresh scrape.")
            self.total_followers_scraped = 0
            self.global_iteration = 0
            self.last_max_id = "0|"
            self.current_cookie_index = -1

        # If cookie_states is empty, load from proxy_cookie_pairs.json
        if not self.cookie_states:
            self.cookie_states = self.load_proxy_cookie_pairs()

    def get_base_encoded_part(self):
        for cookie_state in self.cookie_states:
            try:
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers and 'next_max_id' in followers:
                    self.base_encoded_part = followers['next_max_id'].split('|')[1]
                    return
            except Exception as e:
                print(f"Error fetching initial followers: {e}")
        raise Exception("Failed to get base encoded part from any cookie")

    def scrape_followers(self):
        self.load_state()
        if not self.base_encoded_part:
            self.get_base_encoded_part()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_with_cookie, cookie_state) 
                       for cookie_state in self.cookie_states]
            concurrent.futures.wait(futures)

        print("Scraping complete.")
        self.save_state()

    def scrape_with_cookie(self, cookie_state):
        while not self.stop_event.is_set():
            next_max_id = self.get_next_max_id()
            if next_max_id is None:
                break

            print(f"Fetching with {cookie_state.name}, max_id: {next_max_id}")
            
            params = self.params.copy()
            params['max_id'] = next_max_id
            followers = self.fetch_followers(cookie_state, params)
            
            if followers:
                with self.cookie_state_lock:
                    self.followers.extend(followers['users'])
                    self.total_followers_scraped += len(followers['users'])
                    print(f"Total followers scraped: {self.total_followers_scraped}")
                    self.save_followers(followers['users'])
                    if self.global_iteration >= 3:
                        self.global_iteration += 1  # Increment for small steps
                cookie_state.fail_count = 0  # Reset fail count on success
            else:
                print(f"No followers returned for {cookie_state.name}")
                cookie_state.fail_count += 1
                if cookie_state.fail_count >= 3:
                    cookie_state.active = False
                    print(f"Deactivating {cookie_state.name} due to repeated failures.")
                    break

            self.save_state()

    def fetch_followers(self, cookie_state, params=None, initial_request=False):
        if params is None:
            params = self.params.copy()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 105.0.0.11.118 (iPhone11,8; iOS 12_3_1; en_US; en-US; scale=2.00; 828x1792; 165586599)',
            'Cookie': cookie_state.cookie
        }

        # Parse the proxy string
        proxy_parts = cookie_state.proxy.split(':')
        if len(proxy_parts) == 4:
            proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"
        else:
            proxy_url = f"http://{cookie_state.proxy}"

        proxies = {
            'http': proxy_url,
            'https': proxy_url
        }

        self.wait_with_jitter()
        for _ in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, headers=headers, proxies=proxies)
                response.raise_for_status()
                data = response.json()
                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data or not data['users']:
                    print(f"No more followers found for {cookie_state.name}")
                    return None

                return data
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [400, 429]:
                    print(f"Error {e.response.status_code}: Possible rate limit for {cookie_state.name}. Current max_id: {params.get('max_id')}")
                    return None
                else:
                    print(f"Request failed: {e}")
            except requests.exceptions.RequestException as e:
                print(f"Request exception: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
            
            time.sleep(5)  # Wait before retrying
        
        print(f"Max retries reached for {cookie_state.name}")
        return None

    def wait_with_jitter(self):
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

    def save_followers(self, followers):
        if not followers:
            print("No followers to save.")
            return

        output_dir = 'instagram_followers'
        os.makedirs(output_dir, exist_ok=True)

        filename = f"{output_dir}/followers_{self.user_id}.csv"

        columns_to_save = {
            'pk': 'user_id',
            'username': 'username',
            'full_name': 'full_name',
            'is_private': 'is_private',
            'is_verified': 'is_verified',
            'profile_pic_url': 'profile_pic_url'
        }

        df = pd.DataFrame(followers)
        df_to_save = df[columns_to_save.keys()].rename(columns=columns_to_save)

        # Check if the file exists to determine whether to write headers
        file_exists = os.path.isfile(filename)
        
        # Append to the CSV file without writing the index
        df_to_save.to_csv(filename, mode='a', header=not file_exists, index=False)

        print(f"Appended {len(followers)} followers to {filename}")

    def get_next_max_id(self):
        with self.max_id_lock:
            if self.global_iteration < 3:
                next_count = (self.global_iteration + 1) * self.large_step
                self.global_iteration += 1
            else:
                current_count = int(self.last_max_id.split('|')[0])
                next_count = current_count + self.small_step
            
            self.last_max_id = f"{next_count}|{self.base_encoded_part}"
            return self.last_max_id

    def save_state(self):
        state = {
            'user_id': self.user_id,
            'cookie_states': [
                {
                    'cookie': cs.cookie,
                    'proxy': cs.proxy,
                    'name': cs.name,
                    'index': cs.index,
                    'active': cs.active,
                    'last_request_time': cs.last_request_time,
                    'fail_count': cs.fail_count
                } for cs in self.cookie_states
            ],
            'followers_count': self.total_followers_scraped,
            'base_encoded_part': self.base_encoded_part,
            'global_iteration': self.global_iteration,
            'last_max_id': self.last_max_id,
            'current_cookie_index': self.current_cookie_index
        }
        with open('scraper_state.json', 'w') as f:
            json.dump(state, f)

def main():
    user_id = "25922742395"
    scraper = InstagramScraper(user_id)
    scraper.scrape_followers()

if __name__ == "__main__":
    main()