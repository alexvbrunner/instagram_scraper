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
        self.last_cookie_check = time.time()

    def can_make_request(self):
        current_time = time.time()
        if current_time - self.hour_start >= 3600:
            self.requests_this_hour = 0
            self.hour_start = current_time
        time_since_last_request = current_time - self.last_request_time
        return self.requests_this_hour < 5 and time_since_last_request >= 300  # 5 minutes between requests

    def increment_request_count(self):
        self.requests_this_hour += 1
        self.last_request_time = time.time()

class InstagramScraper:
    def __init__(self, user_id, csv_filename, account_data, db_config):
        self.user_id = user_id
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
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
        self.large_step = 200
        self.small_step = 200
        self.total_followers_scraped = 0
        self.followers = []
        self.gender_detector = gender_detector.Detector()
        self.max_retries = 3
        self.use_proxies = True
        self.current_account_index = None
        self.account_id_to_index = {account['id']: i for i, account in enumerate(self.account_data)}
        self.index_to_account_id = {i: account['id'] for i, account in enumerate(self.account_data)}
        self.rate_limit_info = {}  # New attribute to store rate limit info
        self.available_cookies = queue.Queue()
        self.initialize_available_cookies()
        self.cookie_check_interval = 60  # 1 minute in seconds
        self.rate_limit_threshold = 3  # Number of suspected rate limits before backing off
        self.rate_limit_window = 600  # 10 minutes in seconds
        self.rate_limit_counts = {}  # To track rate limit hits per account
        self.request_lock = threading.Lock()  # Add this line
        self.current_max_id = "0"  # Add this line
        self.unique_followers = set()  # Add this line to track unique followers

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
        try:
            return self.available_cookies.get_nowait()
        except queue.Empty:
            return None

    def return_cookie_to_pool(self, cookie_state):
        self.available_cookies.put(cookie_state)

    def get_next_max_id(self):
        return self.current_max_id

    def update_max_id(self, new_max_id):
        self.current_max_id = new_max_id
        logger.info(f"update_max_id: Updated current_max_id to: {new_max_id}")

    def get_new_cookie_from_db(self, account_id, old_cookie):
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor(dictionary=True)
            query = """
            SELECT cookies 
            FROM accounts 
            WHERE id = %s 
            ORDER BY cookie_timestamp DESC
            LIMIT 1
            """
            cursor.execute(query, (account_id,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Retrieved latest cookie for account ID {account_id}")
                return result['cookies']
            else:
                logger.info(f"No cookie found for account ID {account_id}")
                return None
        except Error as e:
            logger.error(f"Error fetching new cookie from database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
        return None

    def get_dynamic_wait_time(self, account_id):
        rate_info = self.rate_limit_info.get(account_id, {})
        if 'reset_time' in rate_info and time.time() < rate_info['reset_time']:
            return max(rate_info['reset_time'] - time.time(), 300)
        return 300  # Default to 5 minutes if no rate limit info

    def update_rate_limit_info(self, account_id, response_headers):
        if 'X-RateLimit-Remaining' in response_headers:
            remaining = int(response_headers['X-RateLimit-Remaining'])
            reset_time = int(response_headers.get('X-RateLimit-Reset', 0))
            self.rate_limit_info[account_id] = {
                'remaining': remaining,
                'reset_time': reset_time
            }
            logger.info(f"Updated rate limit info for account ID {account_id}: {self.rate_limit_info[account_id]}")

    def main(self):
        self.scrape_followers()
        self.monitor_performance()  # Add performance monitoring at the end

    def get_base_encoded_part(self):
        available_cookies = list(self.cookie_states)
        while available_cookies:
            cookie_state = available_cookies.pop(0)
            account_id = self.index_to_account_id[cookie_state.index]
            try:
                logger.info(f"Attempting to fetch initial followers with account ID {account_id}")
                followers = self.fetch_followers(cookie_state, initial_request=True)
                if followers:
                    logger.info(f"Initial followers response: {json.dumps(followers, indent=2)}")
                    if 'next_max_id' in followers:
                        next_max_id = followers['next_max_id']
                        logger.info(f"next_max_id found: {next_max_id}")
                        self.last_max_id = next_max_id
                        self.base_encoded_part = next_max_id  # Use the entire next_max_id as base_encoded_part
                        logger.info(f"Successfully set base_encoded_part to: {self.base_encoded_part}")
                        return
                    else:
                        logger.info("'next_max_id' not found in response")
                else:
                    logger.info(f"No followers data returned for account ID {account_id}")
            except Exception as e:
                logger.error(f"Error fetching initial followers with account ID {account_id}: {str(e)}")
                logger.error(f"Exception details: {type(e).__name__}")
                logger.error(traceback.format_exc())
                
                # Check if it's a rate limit error
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code in [429, 400]:
                    logger.warning(f"Rate limit hit for account ID {account_id}. Trying next cookie.")
                    continue
            
            # If we reach here, it means we've tried all cookies or encountered a non-rate-limit error
            if not available_cookies:
                raise Exception("Failed to get base encoded part from any cookie")
        
        raise Exception("Failed to get base encoded part from any cookie")

    def scrape_followers(self):
        logger.info("Starting scrape_followers method")
        self.load_state()
        if not self.base_encoded_part:
            logger.info("Base encoded part not found, fetching it now")
            self.get_base_encoded_part()
        else:
            logger.info(f"Using existing base_encoded_part: {self.base_encoded_part}")

        logger.info(f"Starting scraping with {self.max_workers} workers")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_with_cookie, self.get_next_available_cookie()) 
                    for _ in range(self.max_workers)]
            
            save_interval = 100  # Save state every 100 followers
            last_save_time = time.time()
            save_time_interval = 300  # Save state every 5 minutes (300 seconds)
            last_status_log_time = time.time()
            status_log_interval = 60  # Log account status every 1 minute

            try:
                for future in concurrent.futures.as_completed(futures):
                    future.result()
                    
                    current_time = time.time()
                    
                    # Save state based on number of followers scraped
                    if self.total_followers_scraped % save_interval == 0:
                        self.save_state()
                        logger.info(f"State saved after scraping {self.total_followers_scraped} followers")
                    
                    # Save state based on time interval
                    if current_time - last_save_time >= save_time_interval:
                        self.save_state()
                        logger.info(f"State saved after {save_time_interval} seconds")
                        last_save_time = current_time

                    # Log account status
                    if current_time - last_status_log_time >= status_log_interval:
                        self.log_account_status()
                        last_status_log_time = current_time

            except Exception as e:
                logger.error(f"Error in scrape_followers: {str(e)}")
                logger.error(traceback.format_exc())
            finally:
                self.save_state()  # Save state one last time before exiting

        logger.info("Scraping complete.")
        self.save_state()  # Final save after all scraping is done

    def scrape_with_cookie(self, initial_cookie_state):
        cookie_state = initial_cookie_state
        current_account_id = self.index_to_account_id[cookie_state.index]
        logger.info(f"Starting scrape_with_cookie for account ID {current_account_id}")
        last_cookie_check = time.time()
        scraping_complete = False  # Add this flag

        while not self.stop_event.is_set():
            try:
                if time.time() - last_cookie_check > self.cookie_check_interval:
                    cookie_state = self.check_and_update_cookie(cookie_state)
                    last_cookie_check = time.time()

                with self.request_lock:
                    self.check_and_update_cookie(cookie_state)
                    current_max_id = self.get_next_max_id()
                    logger.info(f"scrape_with_cookie: Retrieved current_max_id: {current_max_id} for account ID {current_account_id}")

                    params = self.params.copy()
                    params['max_id'] = current_max_id
                    followers = self.fetch_followers(cookie_state, params)

                    if followers == "RATE_LIMITED":
                        logger.info(f"Rate limit reached for account ID {current_account_id}, switching cookie...")
                        self.increment_rate_limit_count(current_account_id)
                        self.return_cookie_to_pool(cookie_state)
                        
                        # Try to get a new available cookie immediately
                        new_cookie_state = self.get_next_available_cookie()
                        if new_cookie_state is None:
                            logger.warning("No available cookies. Waiting before retry...")
                            time.sleep(30)  # Wait for 30 seconds before checking again
                            continue
                        
                        cookie_state = new_cookie_state
                        current_account_id = self.index_to_account_id[cookie_state.index]
                        logger.info(f"Switched to account ID {current_account_id}")
                        continue  # Retry the same request with the new cookie
                    elif followers is None:
                        logger.info(f"No followers returned for account ID {current_account_id}, max_id: {current_max_id}")
                        break  # End scraping if we get no followers

                    logger.info(f"Successfully fetched followers for account ID {current_account_id}, max_id: {current_max_id}")

                    self.save_followers(followers['users'])

                    new_followers = [follower['username'] for follower in followers['users']]
                    self.unique_followers.update(new_followers)
                    self.followers.extend(followers['users'])
                    self.total_followers_scraped += len(followers['users'])
                    logger.info(f"Total followers scraped: {self.total_followers_scraped}")
                    logger.info(f"Unique followers scraped: {len(self.unique_followers)}")
                    if self.global_iteration >= 3:
                        self.global_iteration += 1  # Increment for small steps

                    logger.info(f"Cumulative followers scraped: {self.total_followers_scraped}")
                    logger.info(f"Cumulative unique followers scraped: {len(self.unique_followers)}")

                    self.monitor_performance()

                    if 'next_max_id' in followers:
                        self.update_max_id(followers['next_max_id'])
                    else:
                        # If no next_max_id is provided, we'll increment it ourselves
                        new_max_id = str(int(current_max_id) + self.params['count'])
                        self.update_max_id(new_max_id)
                        logger.info(f"No 'next_max_id' found. Incrementing max_id to: {new_max_id}")

                    if not followers['users']:
                        logger.info("No more followers to fetch. Ending scraping.")
                        scraping_complete = True  # Set the flag
                        break

            finally:
                self.return_cookie_to_pool(cookie_state)
                logger.info(f"Putting cookie for account ID {current_account_id} back in the queue")

                # Only wait if the current account is rate limited and scraping is not complete
                if not scraping_complete and not cookie_state.can_make_request():
                    wait_time = self.get_dynamic_wait_time(current_account_id)
                    logger.info(f"Waiting {wait_time:.2f} seconds before next request for account ID {current_account_id}")
                    time.sleep(wait_time)
                elif scraping_complete:
                    logger.info("Scraping complete, skipping final wait.")
                else:
                    # If the account can make a request, continue immediately
                    continue

        logger.info(f"Exiting scrape_with_cookie for account ID {current_account_id}")

    def fetch_followers(self, cookie_state, params=None, initial_request=False):
        current_account_id = self.index_to_account_id[cookie_state.index]
        logger.info(f"Entering fetch_followers for account ID {current_account_id}, initial_request: {initial_request}")
        if params is None:
            params = self.params.copy()
        
        logger.info(f"fetch_followers: Request params: {params}")
        
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
        backoff_time = 5  # Start with 5 seconds
        for retry in range(self.max_retries):
            if not cookie_state.can_make_request():
                logger.info(f"Rate limit reached for account ID {current_account_id}, signaling to switch cookie...")
                return "RATE_LIMITED"  # Return a specific signal for rate limiting

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
                
                # Update rate limit info
                self.update_rate_limit_info(current_account_id, response.headers)
                
                response.raise_for_status()
                data = response.json()
                
                logger.info(f"Successfully fetched data")
                logger.info(f"Response data: {json.dumps(data, indent=2)}")
                
                cookie_state.last_request_time = time.time()
                
                if 'users' not in data:
                    logger.warning("'users' key not found in response data")
                elif not data['users']:
                    logger.warning("'users' list is empty in response data")
                
                if 'next_max_id' in data:
                    self.update_max_id(data['next_max_id'])
                else:
                    logger.warning("fetch_followers: 'next_max_id' not found in response data")
                    # If no next_max_id is provided, we'll increment it ourselves
                    current_max_id = params.get('max_id', '0')
                    new_max_id = str(int(current_max_id) + len(data.get('users', [])))
                    self.update_max_id(new_max_id)
                
                return data
            except requests.exceptions.Timeout:
                logger.info(f"Request timed out for account ID {current_account_id}, max_id: {params.get('max_id')}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401 and "Please wait" in e.response.text:
                    logger.warning(f"Rate limit hit for account ID {current_account_id}. Signaling to switch cookie.")
                    return "RATE_LIMITED"  # Return a specific signal for rate limiting
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
            
            logger.info(f"Retrying in 5 seconds...")
            time.sleep(5)  # Wait before retrying
        
        logger.info(f"Max retries reached for account ID {current_account_id}")
        return None

    def wait_with_jitter(self):
        activity_type = random.choices(['quick', 'normal', 'engaged'], weights=[0.5, 0.3, 0.2])[0]
        
        if activity_type == 'quick':
            jitter = np.random.exponential(scale=1)
        elif activity_type == 'normal':
            jitter = np.random.normal(loc=3, scale=1)
        else:  # engaged
            jitter = np.random.normal(loc=5, scale=2)

        # Add micro-breaks
        if random.random() < 0.05:  # 5% chance of a micro-break
            jitter += np.random.uniform(1, 10)  # 10-30 second break

        # Ensure minimum wait time
        jitter = max(jitter, 0.5)

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
            'unique_followers': list(self.unique_followers),  # Add this line
            'cookie_states': {
                self.account_data[cs.index]['id']: {
                    'active': cs.active,
                    'fail_count': cs.fail_count,
                    'requests_this_hour': cs.requests_this_hour,
                    'hour_start': cs.hour_start
                } for cs in self.cookie_states
            }
        }
        with open(f'{self.user_id}_state.json', 'w') as f:
            json.dump(state, f)
        logger.info(f"State saved for user {self.user_id}")

    def load_state(self):
        try:
            with open(f'{self.user_id}_state.json', 'r') as f:
                state = json.load(f)
            self.current_max_id = state['current_max_id']
            self.base_encoded_part = state['base_encoded_part']
            self.global_iteration = state['global_iteration']
            self.total_followers_scraped = state['total_followers_scraped']
            self.unique_followers = set(state.get('unique_followers', []))  # Add this line
            for account_id, cs_state in state['cookie_states'].items():
                if account_id in self.account_id_to_index:
                    index = self.account_id_to_index[account_id]
                    cs = self.cookie_states[index]
                    cs.active = cs_state['active']
                    cs.fail_count = cs_state['fail_count']
                    cs.requests_this_hour = cs_state['requests_this_hour']
                    cs.hour_start = cs_state['hour_start']
            logger.info(f"State loaded for user {self.user_id}")
        except FileNotFoundError:
            logger.info(f"No previous state found for user {self.user_id}")

    def monitor_performance(self):
        total_requests = sum(cs.requests_this_hour for cs in self.cookie_states)
        total_failures = sum(cs.fail_count for cs in self.cookie_states)
        success_rate = (total_requests - total_failures) / total_requests if total_requests > 0 else 0
        
        logger.info(f"Performance Monitor:")
        logger.info(f"Total followers scraped: {self.total_followers_scraped}")
        logger.info(f"Total unique followers scraped: {len(self.unique_followers)}")
        logger.info(f"Total requests made: {total_requests}")
        logger.info(f"Success rate: {success_rate:.2%}")
        logger.info(f"Rate limit info: {self.rate_limit_info}")
        logger.info(f"Rate limit counts: {self.rate_limit_counts}")

        # Display available and rate-limited accounts
        available_accounts = []
        rate_limited_accounts = []
        current_time = time.time()

        for cs in self.cookie_states:
            account_id = self.index_to_account_id[cs.index]
            if cs.can_make_request():
                available_accounts.append(account_id)
            else:
                time_until_available = 300 - (current_time - cs.last_request_time)
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
            else:
                time_until_available = 300 - (current_time - cs.last_request_time)
                rate_limited_accounts.append((account_id, time_until_available))

        logger.info(f"Available accounts: {', '.join(map(str, available_accounts))}")
        logger.info("Rate-limited accounts:")
        for account_id, time_until_available in rate_limited_accounts:
            logger.info(f"  Account ID {account_id}: {time_until_available:.2f} seconds until available")

    def check_and_update_cookie(self, cookie_state):
        account_id = self.index_to_account_id[cookie_state.index]
        logger.info(f"Checking for new cookie for account ID {account_id}")
        new_cookie = self.get_new_cookie_from_db(account_id, cookie_state.cookie)
        if new_cookie and new_cookie != cookie_state.cookie:
            logger.info(f"New cookie found for account ID {account_id}. Updating.")
            new_cookie_state = CookieState(
                new_cookie,
                cookie_state.proxy,
                cookie_state.user_agent,
                cookie_state.index
            )
            new_cookie_state.active = True
            new_cookie_state.fail_count = 0
            new_cookie_state.requests_this_hour = 0
            new_cookie_state.hour_start = time.time()
            new_cookie_state.last_cookie_check = time.time()
            return new_cookie_state
        else:
            logger.info(f"No new cookie found for account ID {account_id}")
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

    scraper = InstagramScraper(user_id, csv_filename, account_data, db_config)
    logger.info(f"Scraping followers for User ID: {user_id}")
    logger.info(f"Using accounts with IDs: {', '.join(str(id) for id in scraper.account_id_to_index.keys())}")
    scraper.main()

if __name__ == "__main__":
    main()

