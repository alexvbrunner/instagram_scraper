import requests
import json
import random
import time
import logging
import mysql.connector
from mysql.connector import Error
import traceback
import gender_guesser.detector as gender
import concurrent.futures
import queue
import heapq
import numpy as np
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

debug_handler = logging.FileHandler('debug_user_data.log')
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
        self.cooldown_time = 30
        self.min_cooldown = 10
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

class InstagramUserDataScraper:
    def __init__(self, user_ids, csv_filename, account_data, db_config):
        self.user_ids = user_ids
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.base_url = "https://i.instagram.com/api/v1/users/{}/info/"
        self.cookie_queue = queue.Queue()
        self.cookie_states = self.initialize_cookie_states()
        self.max_workers = len(self.account_data)
        self.stop_event = threading.Event()
        self.cookie_state_lock = threading.Lock()
        self.gender_detector = gender.Detector()
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
        self.account_wait_times = {}
        self.scraping_status = "in_progress"
        self.scraping_stop_reason = None
        self.start_time = time.time()
        self.total_users_scraped = 0

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

    def scrape_user_data(self):
        logger.info("Starting scrape_user_data method")
        
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
                        logger.info("Scraping complete. Stopping all scraping.")
                        executor.shutdown(wait=False)
                        break

                    if self.total_users_scraped % save_interval == 0:
                        self.save_state()
                        logger.info(f"State saved after scraping {self.total_users_scraped} users")
                    
                    if current_time - last_save_time >= save_time_interval:
                        self.save_state()
                        logger.info(f"State saved after {save_time_interval} seconds")
                        last_save_time = current_time

                    if current_time - last_status_log_time >= status_log_interval:
                        self.log_account_status()
                        last_status_log_time = current_time

            except Exception as e:
                logger.error(f"Error in scrape_user_data: {str(e)}")
                logger.error(traceback.format_exc())
            finally:
                self.save_state()

        logger.info("Scraping complete.")
        self.save_state()

    def scrape_with_cookie(self, initial_cookie_state):
        while not self.stop_event.is_set() and self.user_ids:
            try:
                with self.request_lock:
                    cookie_state = self.get_next_available_cookie()
                    if cookie_state is None:
                        logger.warning("No available cookies. Waiting before retry...")
                        for _ in range(5):
                            if self.stop_event.is_set():
                                logger.info("Stop event detected during wait. Exiting.")
                                return None
                            time.sleep(1)
                        continue

                    current_account_id = self.index_to_account_id[cookie_state.index]
                    logger.debug(f"Starting scrape with account ID {current_account_id}")

                    cookie_state = self.check_and_update_cookie(cookie_state)
                    self.cookie_states[cookie_state.index] = cookie_state

                    user_id = self.user_ids.pop(0)
                    logger.debug(f"Scraping user ID: {user_id} with account ID {current_account_id}")

                    user_data = self.fetch_user_data(cookie_state, user_id)

                    if user_data == "RATE_LIMITED":
                        logger.debug(f"Rate limit reached for account ID {current_account_id}, will switch cookie in next iteration")
                        self.user_ids.append(user_id)  # Put the user_id back in the list
                        continue
                    elif user_data is None:
                        logger.info(f"Failed to fetch data for user ID {user_id}")
                        continue

                    self.process_user_data(user_data, user_id)
                    self.total_users_scraped += 1

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
                    logger.info("Scraping stopped by stop event. Exiting scrape_with_cookie.")
                    return None

        logger.debug(f"Exiting scrape_with_cookie")
        return True if not self.user_ids else False

    def fetch_user_data(self, cookie_state, user_id):
        current_account_id = self.index_to_account_id[cookie_state.index]
        logger.debug(f"Entering fetch_user_data for account ID {current_account_id}, user_id: {user_id}")
        
        url = self.base_url.format(user_id)
        
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

        logger.debug(f"Sending request to {url}")
        if self.use_proxies:
            logger.debug(f"Using proxy: {proxies}")
        else:
            logger.debug("Not using proxy")

        backoff_time = 5
        for retry in range(self.max_retries):
            if not cookie_state.can_make_request():
                logger.debug(f"Cooldown not finished for account ID {current_account_id}, signaling to switch cookie...")
                return "RATE_LIMITED"

            logger.debug(f"Attempt {retry + 1} of {self.max_retries}")
            try:
                response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies, timeout=30)
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
                
                cookie_state.last_request_time = time.time()
                
                if 'user' not in data:
                    logger.warning("'user' key not found in response data. Possible rate limit or API issue.")
                    self.increment_rate_limit_count(current_account_id)
                    return "RATE_LIMITED"

                cookie_state.cooldown_time = max(cookie_state.min_cooldown, 30)
                cookie_state.is_rate_limited = False

                return data

            except requests.exceptions.Timeout:
                logger.info(f"Request timed out for account ID {current_account_id}, user_id: {user_id}")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 401 and "Please wait" in e.response.text:
                    logger.warning(f"Rate limit hit for account ID {current_account_id}. Setting cooldown to 5 minutes.")
                    cookie_state.cooldown_time = max(cookie_state.min_cooldown, 300)  # 5 minutes
                    cookie_state.is_rate_limited = True
                    self.increment_rate_limit_count(current_account_id)
                    return "RATE_LIMITED"
                else:
                    logger.error(f"HTTP Error for account ID {current_account_id}, user_id: {user_id}: {e}")
                    logger.error(f"Response content: {e.response.content}")
                    if e.response.status_code in [400, 429]:
                        logger.error(f"Error {e.response.status_code}: Possible rate limit.")
                        return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Request Exception for account ID {current_account_id}, user_id: {user_id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error for account ID {current_account_id}, user_id: {user_id}: {e}")
                logger.error(traceback.format_exc())
            
            logger.debug(f"Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)
            backoff_time *= 2
        
        logger.info(f"Max retries reached for account ID {current_account_id}")
        return None

    def process_user_data(self, user_data, user_id):
        if 'user' in user_data:
            user_info = user_data['user']
            parsed_data = {
                'user_id': user_id,
                'username': user_info.get('username'),
                'full_name': user_info.get('full_name'),
                'follower_count': user_info.get('follower_count'),
                'following_count': user_info.get('following_count'),
                'media_count': user_info.get('media_count'),
                'is_private': user_info.get('is_private'),
                'is_verified': user_info.get('is_verified'),
                'biography': user_info.get('biography'),
                'external_url': user_info.get('external_url'),
                'gender': self.guess_gender(user_info.get('full_name')),
                'csv_filename': self.csv_filename
            }
            self.save_user_data(parsed_data)
            logger.info(f"Processed and saved data for user ID: {user_id}")
        else:
            logger.warning(f"No user data found for user ID: {user_id}")

    def guess_gender(self, name):
        if name and ' ' in name:
            first_name = name.split()[0]
        else:
            first_name = name
        return self.gender_detector.get_gender(first_name) if first_name else 'unknown'

    def save_user_data(self, user_data):
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO user_data (
                user_id, username, full_name, follower_count, following_count,
                media_count, is_private, is_verified, biography, external_url,
                gender, csv_filename
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON DUPLICATE KEY UPDATE
                username = VALUES(username),
                full_name = VALUES(full_name),
                follower_count = VALUES(follower_count),
                following_count = VALUES(following_count),
                media_count = VALUES(media_count),
                is_private = VALUES(is_private),
                is_verified = VALUES(is_verified),
                biography = VALUES(biography),
                external_url = VALUES(external_url),
                gender = VALUES(gender),
                csv_filename = VALUES(csv_filename)
            """

            data = (
                user_data['user_id'],
                user_data['username'],
                user_data['full_name'],
                user_data['follower_count'],
                user_data['following_count'],
                user_data['media_count'],
                user_data['is_private'],
                user_data['is_verified'],
                user_data['biography'],
                user_data['external_url'],
                user_data['gender'],
                user_data['csv_filename']
            )

            cursor.execute(insert_query, data)
            connection.commit()
            logger.info(f"Saved/Updated user data for user ID: {user_data['user_id']}")

        except Error as e:
            logger.error(f"Error saving user data to database: {e}")

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
            'user_ids': self.user_ids,
            'total_users_scraped': self.total_users_scraped,
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
        with open(f'Files/States/user_data_scraper_state.json', 'w') as f:
            json.dump(state, f)
        logger.info("State saved for user data scraper")

    def load_state(self):
        try:
            with open(f'Files/States/user_data_scraper_state.json', 'r') as f:
                state = json.load(f)
            self.user_ids = state['user_ids']
            self.total_users_scraped = state['total_users_scraped']
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
            logger.info("State loaded for user data scraper")
        except FileNotFoundError:
            logger.info("No previous state found for user data scraper")

    def monitor_performance(self):
        current_time = time.time()
        total_requests = sum(cs.requests_this_hour for cs in self.cookie_states)
        total_failures = sum(cs.fail_count for cs in self.cookie_states)
        success_rate = (total_requests - total_failures) / total_requests if total_requests > 0 else 0
        
        time_elapsed = (current_time - self.start_time) / 3600  # in hours
        users_per_hour = self.total_users_scraped / time_elapsed if time_elapsed > 0 else 0
        
        logger.info(f"-----------------")
        logger.info(f"Performance Monitor:")
        logger.info(f"Total users scraped: {self.total_users_scraped}")
        logger.info(f"Users scraped per hour: {users_per_hour:.2f}")
        logger.info(f"Total requests made: {total_requests}")
        logger.info(f"Success rate: {success_rate:.2%}")
        logger.info(f"Rate limit info: {self.rate_limit_info}")
        logger.info(f"Rate limit counts: {self.rate_limit_counts}")
        logger.info(f"-----------------")

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
                logger.debug(f"Retrieved latest cookie for account ID {account_id}")
                return result['cookies']
            else:
                logger.debug(f"No cookie found for account ID {account_id}")
                return None
        except Error as e:
            logger.error(f"Error fetching new cookie from database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
        return None

    def update_rate_limit_info(self, account_id, response_headers):
        if 'X-RateLimit-Remaining' in response_headers:
            remaining = int(response_headers['X-RateLimit-Remaining'])
            reset_time = int(response_headers.get('X-RateLimit-Reset', 0))
            self.rate_limit_info[account_id] = {
                'remaining': remaining,
                'reset_time': reset_time
            }
            logger.debug(f"Updated rate limit info for account ID {account_id}: {self.rate_limit_info[account_id]}")

def main():
    import sys
    if len(sys.argv) != 5:
        print("Usage: python v4_user_data_scraper.py <user_ids_json> <csv_filename> <account_data_json> <db_config_json>")
        sys.exit(1)

    user_ids = json.loads(sys.argv[1])
    csv_filename = sys.argv[2]
    account_data = sys.argv[3]
    db_config = sys.argv[4]

    scraper = InstagramUserDataScraper(user_ids, csv_filename, account_data, db_config)
    logger.info(f"Scraping user data for {len(user_ids)} users")
    logger.info(f"Using accounts with IDs: {', '.join(str(id) for id in scraper.account_id_to_index.keys())}")
    scraper.scrape_user_data()

if __name__ == "__main__":
    main()