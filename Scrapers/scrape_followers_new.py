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
from requests.exceptions import ProxyError
import mysql.connector
from mysql.connector import Error
import gender_guesser.detector as gender

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CookieState:
    def __init__(self, cookie, index):
        self.cookie = cookie
        self.index = index
        self.active = True
        self.last_request_time = 0
        self.fail_count = 0

class InstagramScraper:
    def __init__(self, user_id, cookies, csv_filename):
        self.user_id = user_id
        self.cookie_states = [CookieState(cookie, i) for i, cookie in enumerate(cookies)]
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
        self.max_workers = len(cookies)
        self.db_connection = self.create_db_connection()
        self.gender_detector = gender.Detector()
        self.csv_filename = csv_filename

    def load_state(self):
        if os.path.exists('Files/scraper_state.json'):
            with open('Files/scraper_state.json', 'r') as f:
                state = json.load(f)
            self.user_id = state['user_id']
            self.cookie_states = []
            for cs in state['cookie_states']:
                cookie_state = CookieState(cs['cookie'], cs['index'])
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

    def get_base_encoded_part(self):
        for cookie_state in self.cookie_states:
            try:
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers:
                    if 'next_max_id' in followers:
                        self.base_encoded_part = followers['next_max_id'].split('|')[-1]
                    elif 'big_list' in followers:
                        self.base_encoded_part = str(followers.get('next_max_id', ''))
                    else:
                        raise ValueError("Unexpected response structure")
                    
                    print(f"Successfully obtained base_encoded_part: {self.base_encoded_part}")
                    return
                else:
                    print(f"No followers data for cookie {cookie_state.index + 1}")
            except Exception as e:
                print(f"Error fetching initial followers for cookie {cookie_state.index + 1}: {e}")
                if 'followers' in locals():
                    print(f"Response content: {json.dumps(followers, indent=2)}")
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

            print(f"Fetching with cookie {cookie_state.index + 1}, max_id: {next_max_id}")
            
            params = self.params.copy()
            params['max_id'] = next_max_id
            followers = self.fetch_followers(cookie_state, params)
            
            if followers:
                with self.cookie_state_lock:
                    self.followers.extend(followers['users'])
                    self.total_followers_scraped += len(followers['users'])
                    print(f"Total followers scraped: {self.total_followers_scraped}")
                    self.save_followers(followers['users'])
                    if 'next_max_id' in followers:
                        self.last_max_id = followers['next_max_id']
                    elif 'big_list' in followers:
                        self.last_max_id = f"{followers.get('next_max_id', '')}|{self.base_encoded_part}"
                    else:
                        print("No next_max_id found in response. Stopping scrape.")
                        break
                cookie_state.fail_count = 0  # Reset fail count on success
            else:
                print(f"No followers returned for cookie {cookie_state.index + 1}")
                cookie_state.fail_count += 1
                if cookie_state.fail_count >= 3:
                    cookie_state.active = False
                    print(f"Deactivating cookie {cookie_state.index + 1} due to repeated failures.")
                    break

            self.save_state()

    def fetch_followers(self, cookie_state, params=None, initial_request=False):
        if params is None:
            params = self.params.copy()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 105.0.0.11.118 (iPhone11,8; iOS 12_3_1; en_US; en-US; scale=2.00; 828x1792; 165586599)',
            'Cookie': cookie_state.cookie
        }

        self.wait_with_jitter()
        for _ in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data or not data['users']:
                    print(f"No more followers found for cookie {cookie_state.index + 1}")
                    return None

                return data
            except requests.exceptions.HTTPError as e:
                if e.response.status_code in [400, 429]:
                    print(f"Error {e.response.status_code}: Possible rate limit for cookie {cookie_state.index + 1}. Current max_id: {params.get('max_id')}")
                    return None
                else:
                    print(f"Request failed: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
            
            time.sleep(5)  # Wait before retrying
        
        print(f"Max retries reached for cookie {cookie_state.index + 1}")
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

        self.insert_followers(followers)
        print(f"Inserted {len(followers)} followers into the database")

    def create_db_connection(self):
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
            return None

    def guess_gender(self, name):
        name_parts = name.split()
        if name_parts:
            first_name = name_parts[0]
            return self.gender_detector.get_gender(first_name)
        return 'unknown'

    def insert_followers(self, followers):
        if not self.db_connection:
            print("No database connection. Skipping database insert.")
            return

        cursor = self.db_connection.cursor()
        query = """
        INSERT INTO followers (source_account, pk, pk_id, username, full_name, is_private, fbid_v2, 
        third_party_downloads_enabled, strong_id, profile_pic_id, profile_pic_url, is_verified, 
        has_anonymous_profile_picture, account_badges, latest_reel_media, is_favorite, gender, csv_filename)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        full_name = VALUES(full_name),
        is_private = VALUES(is_private),
        profile_pic_url = VALUES(profile_pic_url),
        is_verified = VALUES(is_verified),
        gender = VALUES(gender),
        csv_filename = VALUES(csv_filename)
        """
        for follower in followers:
            gender = self.guess_gender(follower.get('full_name', ''))
            data = (
                self.user_id,
                follower.get('pk'),
                follower.get('pk_id'),
                follower.get('username'),
                follower.get('full_name'),
                follower.get('is_private'),
                follower.get('fbid_v2'),
                follower.get('third_party_downloads_enabled'),
                follower.get('strong_id__'),
                follower.get('profile_pic_id'),
                follower.get('profile_pic_url'),
                follower.get('is_verified'),
                follower.get('has_anonymous_profile_picture'),
                json.dumps(follower.get('account_badges')),
                follower.get('latest_reel_media'),
                follower.get('is_favorite'),
                gender,
                self.csv_filename
            )
            cursor.execute(query, data)
        self.db_connection.commit()
        cursor.close()

    def get_next_max_id(self):
        with self.max_id_lock:
            if '|' in self.last_max_id:
                # Original structure
                parts = self.last_max_id.split('|')
                current_count = int(parts[0])
                if self.global_iteration < 3:
                    next_count = (self.global_iteration + 1) * self.large_step
                    self.global_iteration += 1
                else:
                    next_count = current_count + self.small_step
                self.last_max_id = f"{next_count}|{self.base_encoded_part}"
            else:
                # New structure
                if self.global_iteration < 3:
                    self.global_iteration += 1
                # For new structure, we don't modify last_max_id
            return self.last_max_id

    def save_state(self):
        state = {
            'user_id': self.user_id,
            'cookie_states': [
                {
                    'cookie': cs.cookie,
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
        with open('Files/scraper_state.json', 'w') as f:
            json.dump(state, f)

def load_proxy_cookie_pairs():
    with open('Files/proxy_cookie_pairs.json', 'r') as f:
        return json.load(f)

def load_accounts_to_scrape(csv_file):
    df = pd.read_csv(csv_file)
    if 'User ID' in df.columns:
        return df['User ID'].tolist()
    elif 'user_id' in df.columns:
        return df['user_id'].tolist()
    else:
        raise ValueError("CSV file does not contain a 'User ID' or 'user_id' column")

def parse_range(range_str):
    start, end = map(int, range_str.split('-'))
    return start, end + 1  # Add 1 to include the end index

def main():
    filename = input("Enter the csv you want to scrape: ")
    proxy_cookie_pairs = load_proxy_cookie_pairs()
    
    # Ask for the range of proxy/cookie pairs to use
    range_str = input("Enter the range of proxy/cookie pairs to use (e.g., 0-3): ")
    start_index, end_index = parse_range(range_str)
    
    # Slice the proxy_cookie_pairs list based on the input range
    selected_pairs = proxy_cookie_pairs[start_index:end_index]

    print(f'Selected pairs: {selected_pairs}')
    
    if not selected_pairs:
        print(f"No proxy/cookie pairs found in the range {start_index}-{end_index-1}.")
        return

    print(f"Using proxy/cookie pairs {start_index} to {end_index-1}")

    try:
        accounts_to_scrape = load_accounts_to_scrape(f'Files/{filename}')
    except ValueError as e:
        print(f"Error loading accounts: {e}")
        return

    for user_id in accounts_to_scrape:
        print(f"Scraping followers for user_id: {user_id}")
        cookies = [pair['cookie'] for pair in selected_pairs]
        scraper = InstagramScraper(user_id, cookies, filename)
        scraper.scrape_followers()
        print(f"Finished scraping for user_id: {user_id}")
        if scraper.db_connection:
            scraper.db_connection.close()
        time.sleep(60)  # Wait for 1 minute between accounts

if __name__ == "__main__":
    main()