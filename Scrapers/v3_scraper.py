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
import queue
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CookieState:
    def __init__(self, cookie, proxy, user_agent, index):
        self.cookie = cookie
        self.proxy = proxy
        self.user_agent = user_agent
        self.index = index
        self.active = True
        self.last_request_time = 0
        self.fail_count = 0
        self.requests_this_hour = 0
        self.hour_start = time.time()

    def can_make_request(self):
        current_time = time.time()
        if current_time - self.hour_start >= 3600:
            self.requests_this_hour = 0
            self.hour_start = current_time
        return self.requests_this_hour < 5

    def increment_request_count(self):
        self.requests_this_hour += 1
        self.last_request_time = time.time()

class InstagramScraper:
    def __init__(self, user_id, csv_filename, use_proxies, account_data, db_config):
        logger.info('Initializing scraper')
        self.user_id = user_id
        self.csv_filename = csv_filename
        self.use_proxies = use_proxies == 'yes'
        self.account_data = account_data
        self.cookie_states = []
        for idx, account in enumerate(account_data):
            proxy_url = f"{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}"
            self.cookie_states.append(CookieState(account['cookies'], proxy_url, account['user_agent'], idx))
        
        if not self.cookie_states:
            raise ValueError("No valid accounts found. Cannot proceed with scraping.")
        
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
        self.db_config = db_config
        self.gender_detector = gender.Detector()
        self.cookie_queue = queue.Queue()
        for cookie_state in self.cookie_states:
            self.cookie_queue.put(cookie_state)
        logger.info(f"Initializing scraper for user_id: {user_id}, csv_filename: {csv_filename}, use_proxies: {use_proxies}")
        self.cookie_update_interval = 300  # 30 minutes in seconds
        self.cookie_update_thread = threading.Thread(target=self.update_cookies_periodically, daemon=True)
        self.cookie_update_thread.start()

    def load_state(self):
        if os.path.exists('Files/scraper_state.json'):
            with open('Files/scraper_state.json', 'r') as f:
                state = json.load(f)
            self.user_id = state['user_id']
            self.cookie_states = []
            for cs in state['cookie_states']:
                cookie_state = CookieState(
                    cs['cookie'],
                    cs['proxy'],
                    cs['user_agent'],
                    cs['index']
                )
                cookie_state.active = cs['active']
                cookie_state.last_request_time = cs['last_request_time']
                cookie_state.fail_count = cs.get('fail_count', 0)
                cookie_state.requests_this_hour = cs.get('requests_this_hour', 0)
                cookie_state.hour_start = cs.get('hour_start', time.time())
                self.cookie_states.append(cookie_state)
            self.total_followers_scraped = state['followers_count']
            self.base_encoded_part = state.get('base_encoded_part')
            self.global_iteration = state.get('global_iteration', 0)
            self.last_max_id = state.get('last_max_id', "0|")
            self.current_cookie_index = state.get('current_cookie_index', 0)
            logger.info(f"Resumed scraping. Previously scraped {self.total_followers_scraped} followers.")
            logger.info(f"Global iteration: {self.global_iteration}, Last max_id: {self.last_max_id}")
        else:
            logger.info("No previous state found. Starting fresh scrape.")
            self.total_followers_scraped = 0
            self.global_iteration = 0
            self.last_max_id = "0|"
            self.current_cookie_index = 0

        # Reinitialize the cookie queue
        self.cookie_queue = queue.Queue()
        for cookie_state in self.cookie_states:
            self.cookie_queue.put(cookie_state)

    def get_base_encoded_part(self):
        for cookie_state in self.cookie_states:
            try:
                logger.info(f"Attempting to fetch initial followers with {cookie_state.user_agent}")
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers:
                    logger.info(f"Received response: {json.dumps(followers, indent=2)}")
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
                        logger.info(f"Successfully set base_encoded_part to: {self.base_encoded_part}")
                        self.last_max_id = next_max_id
                        return
                    else:
                        logger.info("'next_max_id' not found in response")
                else:
                    logger.info(f"No followers data returned for {cookie_state.user_agent}")
            except Exception as e:
                logger.error(f"Error fetching initial followers with {cookie_state.user_agent}: {str(e)}")
                logger.error(f"Exception details: {type(e).__name__}")
                logger.error(traceback.format_exc())
        
        raise Exception("Failed to get base encoded part from any cookie")

    def scrape_followers(self):
        logger.info("Starting scrape_followers method")
        self.load_state()
        if not self.base_encoded_part:
            logger.info("Base encoded part not found, fetching it now")
            self.get_base_encoded_part()

        logger.info(f"Starting scraping with {self.max_workers} workers")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_with_cookie, self.get_next_available_cookie()) 
                       for _ in range(self.max_workers)]
            concurrent.futures.wait(futures)

        logger.info("Scraping complete.")
        self.save_state()

    def get_next_available_cookie(self):
        backoff = 5
        while True:
            try:
                cookie_state = self.cookie_queue.get(block=False)
                if cookie_state.can_make_request():
                    return cookie_state
                else:
                    self.cookie_queue.put(cookie_state)
                    time.sleep(1)  # Wait before checking the next cookie
            except queue.Empty:
                logger.info("All cookies exhausted. Waiting for fresh cookies...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)  # Exponential backoff, max 5 minutes

    def scrape_with_cookie(self, cookie_state):
        logger.info(f"Starting scrape_with_cookie for {cookie_state.user_agent}")
        while not self.stop_event.is_set():
            if not cookie_state.can_make_request():
                logger.info(f"Cookie {cookie_state.index} exhausted. Waiting for rate limit reset.")
                time.sleep(60)  # Wait for a minute before checking again
                continue

            next_max_id = self.get_next_max_id()
            if next_max_id is None:
                logger.info(f"No more max_id available for {cookie_state.user_agent}, breaking loop")
                break

            logger.info(f"Fetching with {cookie_state.user_agent}, max_id: {next_max_id}")
            
            params = self.params.copy()
            params['max_id'] = next_max_id
            followers = self.fetch_followers(cookie_state, params)
            
            if followers:
                cookie_state.increment_request_count()
                logger.info(f"Successfully fetched {len(followers['users'])} followers with {cookie_state.user_agent}")
                with self.cookie_state_lock:
                    self.followers.extend(followers['users'])
                    self.total_followers_scraped += len(followers['users'])
                    logger.info(f"Total followers scraped: {self.total_followers_scraped}")
                    self.save_followers(followers['users'])
                    if self.global_iteration >= 3:
                        self.global_iteration += 1  # Increment for small steps
                cookie_state.fail_count = 0  # Reset fail count on success
                logger.info(f"Cumulative followers scraped: {self.total_followers_scraped}")
            else:
                logger.info(f"No followers returned for {cookie_state.user_agent}, fail count: {cookie_state.fail_count}")
                cookie_state.fail_count += 1
                if cookie_state.fail_count >= 3:
                    cookie_state.active = False
                    logger.info(f"Deactivating {cookie_state.user_agent} due to repeated failures.")
                    break

            logger.info(f"Saving state after processing with {cookie_state.user_agent}")
            self.save_state()
            self.cookie_queue.put(cookie_state)
            cookie_state = self.get_next_available_cookie()

        logger.info(f"Exiting scrape_with_cookie for {cookie_state.user_agent}")

    def fetch_followers(self, cookie_state, params=None, initial_request=False):
        logger.info(f"Entering fetch_followers for {cookie_state.user_agent}, initial_request: {initial_request}")
        if params is None:
            params = self.params.copy()

        # Updated mobile user agent (Instagram v275.0.0.27.98)
        headers = {
            'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
            'Accept-Language': 'en-US',
            'Accept-Encoding': 'gzip, deflate',
            'X-IG-Capabilities': '3brTvw==',
            'X-IG-Connection-Type': 'WIFI',
            'X-IG-App-ID': '567067343352427',
        }

        # Parse cookies string into a dictionary
        cookies = {}
        for cookie in cookie_state.cookie.split(';'):
            if '=' in cookie:
                name, value = cookie.strip().split('=', 1)
                cookies[name] = value

        logger.info("Using headers:")
        for key, value in headers.items():
            logger.info(f"{key}: {value}")

        logger.info("Using cookies:")
        for key, value in cookies.items():
            masked_value = value[:4] + '*' * (len(value) - 4) if len(value) > 4 else value
            logger.info(f"{key}: {masked_value}")

        proxies = None
        if self.use_proxies:
            proxy_parts = cookie_state.proxy.split('@')
            if len(proxy_parts) == 2:
                auth, address = proxy_parts
                username, password = auth.split(':')
                host, port = address.split(':')
                proxy_url = f"http://{username}:{password}@{host}:{port}"
                proxies = {'http': proxy_url, 'https': proxy_url}
            else:
                logger.error(f"Invalid proxy format: {cookie_state.proxy}")

        logger.info(f"Sending request to {self.base_url} with params: {params}")
        if self.use_proxies:
            logger.info(f"Using proxy: {proxies}")
        else:
            logger.info("Not using proxy")

        self.wait_with_jitter()
        for retry in range(self.max_retries):
            if not cookie_state.can_make_request():
                logger.info(f"Rate limit reached for {cookie_state.user_agent}, waiting...")
                time.sleep(60)
                continue

            logger.info(f"Attempt {retry + 1} of {self.max_retries}")
            try:
                response = requests.get(self.base_url, params=params, headers=headers, cookies=cookies, proxies=proxies, timeout=30)
                cookie_state.increment_request_count()
                logger.info(f"Request status code: {response.status_code}")
                
                # Calculate response size
                response_size = len(response.content)
                logger.info(f"Response size: {response_size} bytes")
                
                # Calculate headers size
                headers_size = len('\r\n'.join(f'{k}: {v}' for k, v in response.headers.items()))
                logger.info(f"Headers size: {headers_size} bytes")
                
                # Calculate total size
                total_size = response_size + headers_size
                logger.info(f"Total size: {total_size} bytes")
                
                response.raise_for_status()
                data = response.json()
                
                logger.info(f"Successfully fetched data")
                logger.info(f"Response data: {json.dumps(data, indent=2)}")
                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data or not data['users']:
                    logger.info(f"No followers found in response")
                    return None

                logger.info(f"Fetched {len(data['users'])} followers")
                return data
            except requests.exceptions.Timeout:
                logger.info(f"Request timed out")
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP Error: {e}")
                logger.error(f"Response content: {e.response.content}")
                if e.response.status_code in [400, 429]:
                    logger.error(f"Error {e.response.status_code}: Possible rate limit. Current max_id: {params.get('max_id')}")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request Exception: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                logger.error(traceback.format_exc())
            
            logger.info(f"Retrying in 5 seconds...")
            time.sleep(5)  # Wait before retrying
        
        logger.info(f"Max retries reached")
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

        logger.info(f"Waiting for {jitter:.2f} seconds.")
        time.sleep(jitter)

    def guess_gender(self, name):
        name_parts = name.split()
        if name_parts:
            first_name = name_parts[0]
            return self.gender_detector.get_gender(first_name)
        return 'unknown'

    def save_followers(self, followers):
        logger.info(f"Entering save_followers method with {len(followers)} followers")
        if not followers:
            logger.info("No followers to save.")
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
            logger.info(f"Inserted/Updated {len(followers)} followers in the database")

        except Error as e:
            logger.error(f"Error saving followers to database: {e}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def get_next_max_id(self):
        logger.info("Entering get_next_max_id method")
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
            
            logger.info(f"Generated next max_id: {self.last_max_id}")
            return self.last_max_id

    def save_state(self):
        logger.info("Saving current state")
        state = {
            'user_id': self.user_id,
            'cookie_states': [
                {
                    'cookie': cs.cookie,
                    'proxy': cs.proxy,
                    'user_agent': cs.user_agent,
                    'index': cs.index,
                    'active': cs.active,
                    'last_request_time': cs.last_request_time,
                    'fail_count': cs.fail_count,
                    'requests_this_hour': cs.requests_this_hour,
                    'hour_start': cs.hour_start
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
        logger.info("State saved successfully")

    def update_cookies_periodically(self):
        while not self.stop_event.is_set():
            time.sleep(self.cookie_update_interval)
            self.update_cookies_from_database()

    def update_cookies_from_database(self):
        logger.info("Updating cookies from database")
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)
            
            for cookie_state in self.cookie_states:
                cursor.execute("""
                    SELECT cookies, user_agent, cookie_timestamp
                    FROM accounts 
                    WHERE proxy_address = %s AND proxy_port = %s
                    AND instagram_created = TRUE 
                    AND cookies IS NOT NULL
                    ORDER BY cookie_timestamp DESC
                    LIMIT 1
                """, (cookie_state.proxy.split(':')[0], cookie_state.proxy.split(':')[1]))
                
                result = cursor.fetchone()
                if result:
                    cookie_state.cookie = result['cookies']
                    cookie_state.user_agent = result['user_agent']
                    logger.info(f"Updated cookie for proxy {cookie_state.proxy}")
                else:
                    logger.info(f"No updated cookie found for proxy {cookie_state.proxy}")
            
            cursor.close()
            connection.close()
        except Error as e:
            logger.error(f"Error updating cookies from database: {e}")

def main():
    logger.info("Entering main function")
    try:
        if len(sys.argv) != 6:
            logger.error("Incorrect number of arguments")
            print("Usage: python v3_scraper.py <user_id> <csv_filename> <use_proxies> <account_data_json> <db_config_json>")
            sys.exit(1)

        user_id, csv_filename, use_proxies, account_data_json, db_config_json = sys.argv[1:]
        
        logger.info(f"Received arguments: user_id={user_id}, csv_filename={csv_filename}, use_proxies={use_proxies}")
        logger.info(f"account_data_json length: {len(account_data_json)}")
        logger.info(f"db_config_json length: {len(db_config_json)}")

        try:
            account_data = json.loads(account_data_json)
            db_config = json.loads(db_config_json)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            logger.error(f"account_data_json: {account_data_json}")
            logger.error(f"db_config_json: {db_config_json}")
            sys.exit(1)

        logger.info("Starting scraper")
        scraper = InstagramScraper(user_id, csv_filename, use_proxies, account_data, db_config)
        scraper.scrape_followers()
        logger.info("Scraping process completed")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
