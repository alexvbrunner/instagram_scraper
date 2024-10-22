import json
import time
import logging
import mysql.connector
from mysql.connector import Error
import requests
import random
import concurrent.futures
import threading
import traceback
import numpy as np
from gender_guesser import detector as gender_detector
import queue
import heapq
from db_utils import (
    get_database_connection,
    get_accounts_from_database,
    prepare_account_data,
    update_account_last_checked,
    mark_account_invalid
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add a separate handler for debug logging
debug_handler = logging.FileHandler('debug.log')
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(debug_formatter)
logger.addHandler(debug_handler)

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
        self.last_cookie_check = time.time()
        self.is_rate_limited = False
        self.cooldown_time = 30  # Default cooldown time in seconds
        self.min_cooldown = 10  # Minimum cooldown time in seconds
        self.max_requests_per_hour = 30

    def can_make_request(self):
        current_time = time.time()
        if current_time - self.hour_start >= 3600:
            self.requests_this_hour = 0
            self.hour_start = current_time
        time_since_last_request = max(self.min_cooldown, current_time - self.last_request_time)
        return self.requests_this_hour < self.max_requests_per_hour and time_since_last_request >= self.cooldown_time

    def increment_request_count(self):
        self.requests_this_hour += 1
        self.last_request_time = time.time()

    def __lt__(self, other):
        return self.last_request_time < other.last_request_time

    def __eq__(self, other):
        return self.last_request_time == other.last_request_time

class InstagramFollowerScraper:
    def __init__(self, user_id, csv_filename, account_data, db_config):
        self.user_id = user_id
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.db_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **self.db_config)
        self.base_url = f"https://i.instagram.com/api/v1/friendships/{self.user_id}/followers/"
        self.params = {"count": 25, "search_surface": "follow_list_page"}
        self.cookie_queue = queue.Queue()
        self.cookie_states = self.initialize_cookie_states()
        self.max_workers = len(self.account_data)
        self.stop_event = threading.Event()
        self.cookie_state_lock = threading.Lock()
        self.last_max_id = "0"
        self.base_encoded_part = ""
        self.global_iteration = 0
        self.large_step = 25
        self.small_step = 25
        self.total_followers_scraped = 0
        self.followers = []
        self.gender_detector = gender_detector.Detector()
        self.max_retries = 3
        self.use_proxies = True
        self.current_account_index = None
        self.account_id_to_index = {account['id']: i for i, account in enumerate(self.account_data)}
        self.index_to_account_id = {i: account['id'] for i, account in enumerate(self.account_data)}
        self.rate_limit_info = {}
        self.available_cookies = queue.Queue()
        self.initialize_available_cookies()
        self.cookie_check_interval = 60
        self.rate_limit_threshold = 3
        self.rate_limit_window = 600
        self.rate_limit_counts = {}
        self.request_lock = threading.Lock()
        self.current_max_id = "0"
        self.unique_followers = set()
        self.account_wait_times = {}
        self.empty_users_count = 0
        self.max_empty_users = 3
        self.scraping_status = "in_progress"
        self.scraping_stop_reason = None
        self.last_unique_followers_count = 0
        self.unchanged_unique_followers_count = 0
        self.max_unchanged_count = 5
        self.last_followers_scraped = 0
        self.start_time = time.time()
        self.manual_increases = 0

    def initialize_cookie_states(self):
        cookie_states = []
        for i, account in enumerate(self.account_data):
            proxy = f"{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}"
            cookie_state = CookieState(account['cookies'], proxy, account['user_agent'], i)
            cookie_states.append(cookie_state)
            self.cookie_queue.put(cookie_state)
        return cookie_states

    def initialize_available_cookies(self):
        for cookie_state in self.cookie_states:
            self.available_cookies.put(cookie_state)

    def get_next_available_cookie(self):
        current_time = time.time()
        available_cookies = []
        for cookie_state in self.cookie_states:
            if not cookie_state.active:
                continue  # Skip inactive accounts
            account_id = self.index_to_account_id[cookie_state.index]
            if cookie_state.can_make_request():
                available_cookies.append((0, current_time, cookie_state))
            else:
                wait_time = max(cookie_state.min_cooldown, cookie_state.cooldown_time - (current_time - cookie_state.last_request_time))
                available_cookies.append((wait_time, current_time, cookie_state))
                self.account_wait_times[account_id] = wait_time

        if not available_cookies:
            return None

        heapq.heapify(available_cookies)
        next_available = heapq.heappop(available_cookies)
        wait_time, _, cookie_state = next_available

        logger.debug(f"Next available cookie: Account ID {self.index_to_account_id[cookie_state.index]}, Wait time: {wait_time:.2f} seconds")
        logger.debug(f"Current wait times for all accounts: {json.dumps(self.account_wait_times, indent=2)}")

        if wait_time > 0:
            logger.info(f"Waiting {wait_time:.2f} seconds before next request")
            time.sleep(wait_time)

        return cookie_state

    def return_cookie_to_pool(self, cookie_state):
        self.available_cookies.put(cookie_state)

    def get_next_max_id(self):
        return self.current_max_id

    def update_max_id(self, new_max_id, manual=False):

        if manual == False:
            self.current_max_id = new_max_id
            logger.info(f"update_max_id: Updated current_max_id to: {new_max_id}")

        else:
            # Convert new_max_id to integer
            new_max_id_int = int(new_max_id)
            
            # Calculate the next divisible value
            remainder = new_max_id_int % self.small_step
            if remainder != 0:
                new_max_id_int += self.small_step - remainder
            
            # Update current_max_id with the adjusted value
            self.current_max_id = str(new_max_id_int)
            logger.info(f"update_max_id: Updated current_max_id to: {self.current_max_id}")

    def get_new_cookie_from_db(self, account_id, old_cookie):
        connection = None
        cursor = None
        try:
            connection = self.db_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            query = """
            SELECT cookies 
            FROM accounts 
            WHERE id = %s 
            ORDER BY last_checked DESC
            LIMIT 1
            """
            cursor.execute(query, (account_id,))
            result = cursor.fetchone()
            if result:
                logger.debug(f"Retrieved latest cookie for account ID {account_id}")
                return result['cookies']
            else:
                logger.debug(f"No cookie found for account ID {account_id}")
                return None
        except Error as e:
            logger.error(f"Error fetching new cookie from database: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return None

    def get_dynamic_wait_time(self, account_id):
        rate_info = self.rate_limit_info.get(account_id, {})
        if 'reset_time' in rate_info and time.time() < rate_info['reset_time']:
            return max(rate_info['reset_time'] - time.time(), 300)
        return 300

    def update_rate_limit_info(self, account_id, response_headers):
        if 'X-RateLimit-Remaining' in response_headers:
            remaining = int(response_headers['X-RateLimit-Remaining'])
            reset_time = int(response_headers.get('X-RateLimit-Reset', 0))
            self.rate_limit_info[account_id] = {
                'remaining': remaining,
                'reset_time': reset_time
            }
            logger.debug(f"Updated rate limit info for account ID {account_id}: {self.rate_limit_info[account_id]}")

    def main(self):
        self.scrape_followers()
        self.monitor_performance()

    def get_base_encoded_part(self):
        available_cookies = list(self.cookie_states)
        while available_cookies:
            cookie_state = available_cookies.pop(0)
            account_id = self.index_to_account_id[cookie_state.index]
            try:
                if self.global_iteration < 3:
                    next_count = (self.global_iteration + 1) * self.large_step
                    self.global_iteration += 1
                self.params['max_id'] = str(next_count)
                logger.info(f"Attempting to fetch initial followers with account ID {account_id}")
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers == "RATE_LIMITED":
                    logger.info(f"Rate limit reached for account ID {account_id}, trying next cookie")
                    continue
                if followers:
                    logger.info(f"Initial followers response: {json.dumps(followers, indent=2)}")
                    if 'next_max_id' in followers:
                        next_max_id = followers['next_max_id']
                        logger.info(f"next_max_id found: {next_max_id}")
                        self.last_max_id = next_max_id
                        self.base_encoded_part = next_max_id
                        logger.info(f"Successfully set base_encoded_part to: {self.base_encoded_part}")
                        return
                    elif 'users' in followers and followers['users']:
                        logger.info("'next_max_id' not found in response")
                        self.current_max_id = str(int(self.current_max_id) + self.large_step)
                        logger.info(f"Updated current_max_id to: {self.current_max_id}")
                        return
                else:
                    self.empty_users_count += 1
                    logger.info(f"No followers data returned for account ID {account_id}, empty_users_count: {self.empty_users_count}")
                    
                # Check if empty_users_count has reached max_empty_users
                if self.empty_users_count >= self.max_empty_users:
                    logger.info(f"Reached {self.max_empty_users} consecutive empty users lists during initial request. Stopping scraping.")
                    self.scraping_status = "stopped"
                    self.scraping_stop_reason = "consecutive_empty_users"
                    self.stop_event.set()
                    self.save_state()
                    return
                    
            except Exception as e:
                logger.error(f"Error fetching initial followers with account ID {account_id}: {str(e)}")
                logger.error(f"Exception details: {type(e).__name__}")
                logger.error(traceback.format_exc())
                
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in [429, 400]:
                    logger.warning(f"Rate limit hit for account ID {account_id}. Trying next cookie.")
                    continue
            
            if not available_cookies:
                error_message = "Failed to get base encoded part from any cookie"
                logger.error(error_message)
                self.scraping_status = "error"
                self.scraping_stop_reason = error_message
                self.stop_event.set()
                self.save_state()  # Save the state to capture this error
                raise Exception(error_message)

    def scrape_followers(self):
        logger.info("Starting scrape_followers method")
        self.load_state()
        if not self.base_encoded_part:
            logger.info("Base encoded part not found, fetching it now")
            try:
                self.get_base_encoded_part()
            except Exception as e:
                logger.error(f"Failed to get base encoded part: {str(e)}")
                return  # Exit the method if we can't get the base encoded part
        else:
            logger.debug(f"Using existing base_encoded_part: {self.base_encoded_part}")

        logger.info(f"--------Starting scraping with {self.max_workers} workers---------")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_with_cookie, self.get_next_available_cookie()) 
                    for _ in range(self.max_workers)]
            
            save_interval = 100
            last_save_time = time.time()
            save_time_interval = 300
            last_status_log_time = time.time()
            status_log_interval = 60

            try:
                for future in concurrent.futures.as_completed(futures):
                    scraping_complete = future.result()
                    
                    current_time = time.time()
                    
                    if scraping_complete:
                        logger.info(f"Scraping complete due to {self.max_empty_users} consecutive empty users lists. Stopping all scraping.")
                        executor.shutdown(wait=False)
                        break

                    if self.total_followers_scraped % save_interval == 0:
                        self.save_state()
                        logger.info(f"State saved after scraping {self.total_followers_scraped} followers")
                    
                    if current_time - last_save_time >= save_time_interval:
                        self.save_state()
                        logger.info(f"State saved after {save_time_interval} seconds")
                        last_save_time = current_time

                    if current_time - last_status_log_time >= status_log_interval:
                        self.log_account_status()
                        last_status_log_time = current_time

            except Exception as e:
                logger.error(f"Error in scrape_followers: {str(e)}")
                logger.error(traceback.format_exc())
            finally:
                self.save_state()

        logger.info("Scraping complete.")
        self.save_state()

    def scrape_with_cookie(self, initial_cookie_state):
        scraping_complete = False

        while not self.stop_event.is_set():
            try:
                with self.request_lock:
                    cookie_state = self.get_next_available_cookie()
                    if cookie_state is None:
                        logger.warning("No available cookies. Waiting before retry...")
                        for _ in range(5):  # Wait in 1-second intervals
                            if self.stop_event.is_set():
                                logger.info("Stop event detected during wait. Exiting.")
                                return None
                            time.sleep(1)
                        continue

                    current_account_id = self.index_to_account_id[cookie_state.index]
                    logger.debug(f"Starting scrape with account ID {current_account_id}")

                    cookie_state = self.check_and_update_cookie(cookie_state)
                    # Update the cookie_state in self.cookie_states
                    self.cookie_states[cookie_state.index] = cookie_state

                    current_max_id = self.get_next_max_id()
                    logger.debug(f"scrape_with_cookie: Retrieved current_max_id: {current_max_id} for account ID {current_account_id}")

                    params = self.params.copy()
                    params['max_id'] = current_max_id
                    followers = self.fetch_followers(cookie_state, params)

                    if followers == "RATE_LIMITED":
                        logger.debug(f"Rate limit reached for account ID {current_account_id}, will switch cookie in next iteration")
                        continue
                    elif followers is None:
                        if self.scraping_status == "stopped" and self.scraping_stop_reason == "consecutive_empty_users":
                            logger.info(f"Scraping complete due to {self.max_empty_users} consecutive empty users lists. Stopping all scraping.")
                            scraping_complete = True
                            self.stop_event.set()
                            break
                        logger.info(f"No more followers to fetch for account ID {current_account_id}")
                        self.scraping_status = "completed"
                        self.scraping_stop_reason = "no_more_followers"
                        break

                    logger.debug(f"Successfully fetched followers for account ID {current_account_id}, max_id: {current_max_id}")

                    self.save_followers(followers['users'])

                    new_followers = [follower['username'] for follower in followers['users']]
                    self.unique_followers.update(new_followers)
                    self.followers.extend(followers['users'])
                    self.total_followers_scraped += len(followers['users'])
                    logger.info(f"Total followers scraped: {self.total_followers_scraped}")
                    logger.info(f"Unique followers scraped: {len(self.unique_followers)}")
                    logger.info(f"Cumulative followers scraped: {self.total_followers_scraped}")
                    logger.info(f"Cumulative unique followers scraped: {len(self.unique_followers)}")

            except Exception as e:
                logger.error(f"Unexpected error in scrape_with_cookie: {str(e)}")
                logger.error(traceback.format_exc())
                self.scraping_status = "error"
                self.scraping_stop_reason = str(e)
                break

            finally:
                if not self.stop_event.is_set():
                    self.return_cookie_to_pool(cookie_state)
                    logger.debug(f"Putting cookie for account ID {current_account_id} back in the queue")
                    self.monitor_performance()
                else:
                    logger.info("Scraping stopped by stop event. Exiting fetch_followers.")
                    return None

        logger.debug(f"Exiting scrape_with_cookie")
        return scraping_complete

    def fetch_followers(self, cookie_state, params=None, initial_request=False):
        current_account_id = self.index_to_account_id[cookie_state.index]
        logger.debug(f"Entering fetch_followers for account ID {current_account_id}, initial_request: {initial_request}")
        if params is None:
            params = self.params.copy()
        
        logger.debug(f"fetch_followers: Request params: {params}")
        
        headers = {
            'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
            'Accept-Language': 'en-US',
            'Accept-Encoding': 'gzip, deflate',
            'X-IG-Capabilities': '3brTvw==',
            'X-IG-Connection-Type': 'WIFI',
            'X-IG-App-ID': '567067343352427',
        }

        cookies = {}
        for cookie in cookie_state.cookie.split(';'):
            if '=' in cookie:
                name, value = cookie.strip().split('=', 1)
                cookies[name] = value

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

        logger.debug(f"+++++++Sending request to {self.base_url} with params: {params}+++++++")
        if self.use_proxies:
            logger.debug(f"Using proxy: {proxies}")
        else:
            logger.debug("Not using proxy")

        # self.wait_with_jitter()
        backoff_time = 5
        for retry in range(self.max_retries):
            if not cookie_state.can_make_request():
                logger.debug(f"Cooldown not finished for account ID {current_account_id}, signaling to switch cookie...")
                return "RATE_LIMITED"

            logger.debug(f"Attempt {retry + 1} of {self.max_retries}")
            try:
                logger.info(f'++++++++Trying request with account ID {current_account_id} and max_id: {params.get('max_id')}+++++++++')
                response = requests.get(self.base_url, params=params, headers=headers, cookies=cookies, proxies=proxies, timeout=30)
                cookie_state.increment_request_count()
                logger.info(f"Request status code: {response.status_code}")
                
                response_size = len(response.content)
                logger.debug(f"Response size: {response_size} bytes")
                
                headers_size = len('\r\n'.join(f'{k}: {v}' for k, v in response.headers.items()))
                logger.debug(f"Headers size: {headers_size} bytes")
                
                total_size = response_size + headers_size
                logger.debug(f"Total size: {total_size} bytes")
                
                self.update_rate_limit_info(current_account_id, response.headers)
                
                response.raise_for_status()
                data = response.json()
                
                logger.debug(f"Successfully fetched data")
                logger.info(f"Response data: {json.dumps(data, indent=2)}")
                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data:
                    logger.warning("'users' key not found in response data. Possible rate limit or API issue.")
                    self.increment_rate_limit_count(current_account_id)
                    return "RATE_LIMITED"
                
                if not data['users']:
                    self.empty_users_count += 1
                    logger.info(f"Empty users list received. Consecutive empty count: {self.empty_users_count}")
                    if self.empty_users_count >= self.max_empty_users:
                        logger.info(f"Received {self.max_empty_users} consecutive empty users lists. Stopping scraping.")
                        self.stop_event.set()
                        self.scraping_status = "stopped"
                        self.scraping_stop_reason = "consecutive_empty_users"
                        self.stop_event.set()
                        return None
                else:
                    self.empty_users_count = 0
                
                if 'next_max_id' in data:
                    self.update_max_id(data['next_max_id'])
                else:
                    logger.warning("fetch_followers: 'next_max_id' not found in response data")
                    current_max_id = params.get('max_id', '0')
                   
                    try:
                        if '|' in current_max_id:
                            numeric_part = int(current_max_id.split('|')[0])
                            new_max_id = str(numeric_part + self.small_step)
                        else:
                            new_max_id = str(int(current_max_id) + self.small_step)
                        
                        logger.info(f'Current max_id: {current_max_id}, length of users: {len(data.get("users", []))}, new_max_id: {new_max_id}')
                        self.update_max_id(new_max_id, manual=True)
                        logger.info(f"Incremented max_id manually to: {new_max_id}")
                        return data
                    except ValueError as e:
                        logger.error(f"Unable to increment max_id: {current_max_id}, error: {e}")
                        return None

                # If the request was successful, reset the cooldown time to 30 seconds
                cookie_state.cooldown_time = max(cookie_state.min_cooldown, 30)
                cookie_state.is_rate_limited = False

                return data
            
            
            except requests.exceptions.Timeout:
                logger.info(f"Request timed out for account ID {current_account_id}, max_id: {params.get('max_id')}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 400:
                    response_content = e.response.content.decode('utf-8')
                    if "challenge_required" in response_content:
                        logger.error(f"Challenge required for account ID {current_account_id}. Disabling this account for the session.")
                        cookie_state.active = False
                        self.save_state()  # Save the updated state
                        return None  # This will cause the scraper to move to the next account
                    else:
                        logger.error(f"HTTP Error 400 for account ID {current_account_id}, max_id: {params.get('max_id')}: {e}")
                        logger.error(f"Response content: {response_content}")
                        return None
                elif e.response.status_code == 401 and "Please wait" in e.response.text:
                    logger.warning(f"Rate limit hit for account ID {current_account_id}. Setting cooldown to 5 minutes.")
                    cookie_state.cooldown_time = max(cookie_state.min_cooldown, 300)  # 5 minutes
                    cookie_state.is_rate_limited = True
                    self.increment_rate_limit_count(current_account_id)
                    return "RATE_LIMITED"
                else:
                    logger.error(f"HTTP Error for account ID {current_account_id}, max_id: {params.get('max_id')}: {e}")
                    logger.error(f"Response content: {e.response.content}")
                    if e.response.status_code in [400, 429]:
                        logger.error(f"Error {e.response.status_code}: Possible rate limit. Current max_id: {params.get('max_id')}")
                        return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request Exception for account ID {current_account_id}, max_id: {params.get('max_id')}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error for account ID {current_account_id}, max_id: {params.get('max_id')}: {e}")
                logger.error(traceback.format_exc())
            
            logger.debug(f"Retrying in 5 seconds...")
            time.sleep(5)
        
        logger.info(f"Max retries reached for account ID {current_account_id}")
        return None

    def wait_with_jitter(self):
        activity_type = random.choices(['quick', 'normal', 'engaged'], weights=[0.5, 0.3, 0.2])[0]
        
        if activity_type == 'quick':
            jitter = np.random.exponential(scale=1)
        elif activity_type == 'normal':
            jitter = np.random.normal(loc=3, scale=1)
        else:
            jitter = np.random.normal(loc=5, scale=2)

        if random.random() < 0.05:
            jitter += np.random.uniform(1, 10)

        jitter = max(jitter, 0.5)

        logger.debug(f"Waiting for {jitter:.2f} seconds.")
        time.sleep(jitter)

    def guess_gender(self, name):
        name_parts = name.split()
        if name_parts:
            first_name = name_parts[0]
            return self.gender_detector.get_gender(first_name)
        return 'unknown'

    def save_followers(self, followers):
        logger.debug(f"Entering save_followers method with {len(followers)} followers")
        if not followers:
            logger.debug("No followers to save.")
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

    def increment_rate_limit_count(self, account_id):
        current_time = time.time()
        self.rate_limit_counts[account_id] = [t for t in self.rate_limit_counts.get(account_id, []) if current_time - t < self.rate_limit_window]
        self.rate_limit_counts[account_id].append(current_time)

    def should_backoff(self, account_id):
        return len(self.rate_limit_counts.get(account_id, [])) >= self.rate_limit_threshold

    def reset_rate_limit_count(self, account_id):
        self.rate_limit_counts[account_id] = []

    def save_state(self):
        state = {
            'current_max_id': self.current_max_id,
            'base_encoded_part': self.base_encoded_part,
            'global_iteration': self.global_iteration,
            'total_followers_scraped': self.total_followers_scraped,
            'unique_followers': list(self.unique_followers),
            'cookie_states': {
                self.account_data[cs.index]['id']: {
                    'active': cs.active,
                    'fail_count': cs.fail_count,
                    'requests_this_hour': cs.requests_this_hour,
                    'hour_start': cs.hour_start
                } for cs in self.cookie_states
            },
            'scraping_status': self.scraping_status,
            'scraping_stop_reason': self.scraping_stop_reason,
            'start_time': self.start_time
        }
        with open(f'Files/States/{self.user_id}_state.json', 'w') as f:
            json.dump(state, f)
        logger.info(f"State saved for user {self.user_id}")

    def load_state(self):
        try:
            with open(f'Files/States/{self.user_id}_state.json', 'r') as f:
                state = json.load(f)
            self.current_max_id = state['current_max_id']
            self.base_encoded_part = state['base_encoded_part']
            self.global_iteration = state['global_iteration']
            self.total_followers_scraped = state['total_followers_scraped']
            self.unique_followers = set(state.get('unique_followers', []))
            for account_id, cs_state in state['cookie_states'].items():
                if account_id in self.account_id_to_index:
                    index = self.account_id_to_index[account_id]
                    cs = self.cookie_states[index]
                    cs.active = cs_state['active']
                    cs.fail_count = cs_state['fail_count']
                    cs.requests_this_hour = cs_state['requests_this_hour']
                    cs.hour_start = cs_state['hour_start']
            self.scraping_status = state.get('scraping_status', 'in_progress')
            self.scraping_stop_reason = state.get('scraping_stop_reason', None)
            self.start_time = state.get('start_time', time.time())
            logger.info(f"State loaded for user {self.user_id}")
        except FileNotFoundError:
            logger.info(f"No previous state found for user {self.user_id}")

    def monitor_performance(self):
        current_time = time.time()
        total_requests = sum(cs.requests_this_hour for cs in self.cookie_states)
        total_failures = sum(cs.fail_count for cs in self.cookie_states)
        success_rate = (total_requests - total_failures) / total_requests if total_requests > 0 else 0
        
        current_unique_followers_count = len(self.unique_followers)
        
        # Calculate time elapsed in different units
        time_elapsed_minutes = (current_time - self.start_time) / 60
        time_elapsed_hours = time_elapsed_minutes / 60
        time_elapsed_days = time_elapsed_hours / 24

        # Calculate unique users per minute, hour, and day
        unique_users_per_minute = current_unique_followers_count / time_elapsed_minutes if time_elapsed_minutes > 0 else 0
        unique_users_per_hour = current_unique_followers_count / time_elapsed_hours if time_elapsed_hours > 0 else 0
        unique_users_per_day = current_unique_followers_count / time_elapsed_days if time_elapsed_days > 0 else 0
        
        # Calculate percentage loss between total and unique followers
        percentage_loss = ((self.total_followers_scraped - current_unique_followers_count) / self.total_followers_scraped * 100) if self.total_followers_scraped > 0 else 0
        
        logger.info(f"-----------------")
        logger.info(f"Performance Monitor:")
        logger.info(f"Total followers scraped: {self.total_followers_scraped}")
        logger.info(f"Total unique followers scraped: {current_unique_followers_count}")
        logger.info(f"Percentage loss: {percentage_loss:.2f}%")
        logger.info(f"Unique users per minute: {unique_users_per_minute:.2f}")
        logger.info(f"Unique users per hour: {unique_users_per_hour:.2f}")
        logger.info(f"Unique users per day: {unique_users_per_day:.2f}")
        logger.info(f"Total requests made: {total_requests}")
        logger.info(f"Success rate: {success_rate:.2%}")
        logger.info(f"Rate limit info: {self.rate_limit_info}")
        logger.info(f"Rate limit counts: {self.rate_limit_counts}")
        logger.info(f'Consecutive empty users lists: {self.empty_users_count}')
        logger.info(f"-----------------")

        if current_unique_followers_count > self.last_unique_followers_count:
            logger.info(f"Unique followers increased from {self.last_unique_followers_count} to {current_unique_followers_count}")
            self.unchanged_unique_followers_count = 0
        elif current_unique_followers_count == self.last_unique_followers_count and self.total_followers_scraped != self.last_followers_scraped:
            self.unchanged_unique_followers_count += 1
            logger.info(f"Unique followers unchanged. Consecutive unchanged count: {self.unchanged_unique_followers_count}")
        else:
            logger.warning(f"Unique followers decreased from {self.last_unique_followers_count} to {current_unique_followers_count}. This should not happen.")

        if self.unchanged_unique_followers_count >= 3:
            logger.warning("Unique followers count unchanged for 3 consecutive checks. Increasing max_id by 100.")
            current_max_id = int(self.current_max_id.split('|')[0]) if '|' in self.current_max_id else int(self.current_max_id)
            new_max_id = str(current_max_id + 100)
            self.update_max_id(new_max_id, manual=True)
            self.unchanged_unique_followers_count = 0
            self.manual_increases += 1

        if self.manual_increases >= self.max_unchanged_count:
            logger.warning(f"Unique followers count unchanged for {self.max_unchanged_count} consecutive checks. Triggering stop event.")
            self.stop_event.set()
            self.scraping_status = "stopped"
            self.scraping_stop_reason = "no_new_unique_followers"
        
        self.last_unique_followers_count = current_unique_followers_count
        self.last_followers_scraped = self.total_followers_scraped

        available_accounts = []
        rate_limited_accounts = []
        current_time = time.time()

        for cs in self.cookie_states:
            account_id = self.index_to_account_id[cs.index]
            if cs.can_make_request():
                available_accounts.append(account_id)
            else:
                time_until_available = max(cs.min_cooldown, cs.cooldown_time - (current_time - cs.last_request_time))
                rate_limited_accounts.append((account_id, time_until_available))

        logger.info(f"Available accounts: {', '.join(map(str, available_accounts))}")
        logger.info("Rate-limited accounts:")
        for account_id, time_until_available in rate_limited_accounts:
            logger.info(f"  Account ID {account_id}: {time_until_available:.2f} seconds until available")

    def log_account_status(self):
        available_accounts = []
        rate_limited_accounts = []
        current_time = time.time()

        for cs in self.cookie_states:
            account_id = self.index_to_account_id[cs.index]
            if cs.can_make_request():
                available_accounts.append(account_id)
                self.account_wait_times[account_id] = 0
            else:
                time_until_available = max(cs.min_cooldown, cs.cooldown_time - (current_time - cs.last_request_time))
                rate_limited_accounts.append((account_id, time_until_available))
                self.account_wait_times[account_id] = time_until_available

        logger.info(f"Available accounts: {', '.join(map(str, available_accounts))}")
        logger.info("Rate-limited accounts:")
        for account_id, time_until_available in rate_limited_accounts:
            logger.info(f"  Account ID {account_id}: {time_until_available:.2f} seconds until available")

    def check_and_update_cookie(self, cookie_state):
        account_id = self.index_to_account_id[cookie_state.index]
        logger.debug(f"Checking for new cookie for account ID {account_id}")
        new_cookie = self.get_new_cookie_from_db(account_id, cookie_state.cookie)
        if new_cookie and new_cookie != cookie_state.cookie:
            logger.info(f"New cookie found for account ID {account_id}. Updating.")
            logger.info(f'New cookie: {new_cookie}')
            logger.info(f'Current cookie: {cookie_state.cookie}')
            cookie_state.cookie = new_cookie
            cookie_state.active = True
            cookie_state.fail_count = 0
            cookie_state.requests_this_hour = 0
            cookie_state.hour_start = time.time()
            cookie_state.last_cookie_check = time.time()
            self.account_wait_times[account_id] = 30
        else:
            logger.debug(f"No new cookie found for account ID {account_id}")
        return cookie_state

def main():
    import sys
    if len(sys.argv) != 5:
        print("Usage: python v4_scraper.py <user_id> <csv_filename> <account_data_json> <db_config_json>")
        sys.exit(1)

    user_id = sys.argv[1]
    csv_filename = sys.argv[2]
    account_data = sys.argv[3]
    db_config = sys.argv[4]

    scraper = InstagramFollowerScraper(user_id, csv_filename, account_data, db_config)
    logger.info(f"Scraping followers for User ID: {user_id}")
    logger.info(f"Using accounts with IDs: {', '.join(str(id) for id in scraper.account_id_to_index.keys())}")
    scraper.main()

if __name__ == "__main__":
    main()