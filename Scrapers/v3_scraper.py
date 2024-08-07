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
import mysql.connector
from mysql.connector import Error
import sys
import gender_guesser.detector as gender

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
    def __init__(self, user_id, csv_filename):
        self.user_id = user_id
        self.csv_filename = csv_filename
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
        self.db_config = {
            'host': '127.0.0.1',
            'user': 'root',
            'password': 'password',
            'database': 'main'
        }
        self.gender_detector = gender.Detector()

    def load_proxy_cookie_pairs(self):
        with open('Files/proxy_cookie_pairs.json', 'r') as f:
            pairs = json.load(f)
        return [CookieState(pair['cookie'], pair['proxy'], pair['name'], i) for i, pair in enumerate(pairs)]

    def load_state(self):
        if os.path.exists('Files/scraper_state.json'):
            with open('Files/scraper_state.json', 'r') as f:
                state = json.load(f)
            self.user_id = state['user_id']
            self.cookie_states = []
            for cs in state['cookie_states']:
                cookie_state = CookieState(cs['cookie'], cs['proxy'], cs['name'], cs['index'])
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
                print(f"Attempting to fetch initial followers with {cookie_state.name}")
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers:
                    print(f"Received response: {json.dumps(followers, indent=2)}")
                    if 'next_max_id' in followers:
                        next_max_id = followers['next_max_id']
                        if '|' in next_max_id:
                            next_max_id_parts = next_max_id.split('|')
                            if len(next_max_id_parts) > 1:
                                self.base_encoded_part = next_max_id_parts[1]
                            else:
                                self.base_encoded_part = ""
                        else:
                            self.base_encoded_part = ""
                        print(f"Successfully set base_encoded_part to: {self.base_encoded_part}")
                        self.last_max_id = next_max_id
                        return
                    else:
                        print("'next_max_id' not found in response")
                else:
                    print(f"No followers data returned for {cookie_state.name}")
            except Exception as e:
                print(f"Error fetching initial followers with {cookie_state.name}: {str(e)}")
                print(f"Exception details: {type(e).__name__}")
                import traceback
                traceback.print_exc()
        
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

        print(f"Sending request to {self.base_url} with params: {params}")
        print(f"Using proxy: {proxy_url}")

        self.wait_with_jitter()
        for _ in range(self.max_retries):
            try:
                response = requests.get(self.base_url, params=params, headers=headers, proxies=proxies)
                response.raise_for_status()
                data = response.json()
                                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data or not data['users']:
                    print(f"No followers found in response for {cookie_state.name}")
                    return None

                return data
            except requests.exceptions.HTTPError as e:
                print(f"HTTP Error: {e}")
                if e.response.status_code in [400, 429]:
                    print(f"Error {e.response.status_code}: Possible rate limit for {cookie_state.name}. Current max_id: {params.get('max_id')}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"Request Exception: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
                import traceback
                traceback.print_exc()
            
            print(f"Retrying in 5 seconds...")
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

    def guess_gender(self, name):
        name_parts = name.split()
        if name_parts:
            first_name = name_parts[0]
            return self.gender_detector.get_gender(first_name)
        return 'unknown'

    def save_followers(self, followers):
        if not followers:
            print("No followers to save.")
            return

        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO followers (
                username, source_account, pk, pk_id, full_name, is_private, fbid_v2,
                third_party_downloads_enabled, strong_id, profile_pic_id, profile_pic_url,
                is_verified, has_anonymous_profile_picture, account_badges, latest_reel_media,
                is_favorite, gender, csv_filename
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                source_account = VALUES(source_account),
                pk = VALUES(pk),
                pk_id = VALUES(pk_id),
                full_name = VALUES(full_name),
                is_private = VALUES(is_private),
                fbid_v2 = VALUES(fbid_v2),
                third_party_downloads_enabled = VALUES(third_party_downloads_enabled),
                strong_id = VALUES(strong_id),
                profile_pic_id = VALUES(profile_pic_id),
                profile_pic_url = VALUES(profile_pic_url),
                is_verified = VALUES(is_verified),
                has_anonymous_profile_picture = VALUES(has_anonymous_profile_picture),
                account_badges = VALUES(account_badges),
                latest_reel_media = VALUES(latest_reel_media),
                is_favorite = VALUES(is_favorite),
                gender = VALUES(gender),
                csv_filename = VALUES(csv_filename)
            """

            for follower in followers:
                gender = self.guess_gender(follower.get('full_name', ''))
                data = (
                    follower.get('username'),
                    self.user_id,
                    follower.get('pk'),
                    follower.get('pk_id'),
                    follower.get('full_name'),
                    follower.get('is_private'),
                    follower.get('fbid_v2'),
                    follower.get('third_party_downloads_enabled'),
                    follower.get('strong_id'),
                    follower.get('profile_pic_id'),
                    follower.get('profile_pic_url'),
                    follower.get('is_verified'),
                    follower.get('has_anonymous_profile_picture'),
                    json.dumps(follower.get('account_badges', [])),
                    follower.get('latest_reel_media'),
                    follower.get('is_favorite'),
                    gender,
                    self.csv_filename
                )
                cursor.execute(insert_query, data)

            connection.commit()
            print(f"Inserted/Updated {len(followers)} followers in the database")

        except Error as e:
            print(f"Error saving followers to database: {e}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def get_next_max_id(self):
        with self.max_id_lock:
            if '|' in self.last_max_id:
                current_count = int(self.last_max_id.split('|')[0])
            else:
                current_count = int(self.last_max_id)
            
            if self.global_iteration < 3:
                next_count = (self.global_iteration + 1) * self.large_step
                self.global_iteration += 1
            else:
                next_count = current_count + self.small_step
            
            if self.base_encoded_part:
                self.last_max_id = f"{next_count}|{self.base_encoded_part}"
            else:
                self.last_max_id = str(next_count)
            
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
        with open('Files/scraper_state.json', 'w') as f:
            json.dump(state, f)

def main():
    if len(sys.argv) != 3:
        print("Usage: python v3_scraper.py <user_id> <csv_filename>")
        sys.exit(1)

    user_id = sys.argv[1]
    csv_filename = sys.argv[2]
    scraper = InstagramScraper(user_id, csv_filename)
    scraper.scrape_followers()

if __name__ == "__main__":
    main()