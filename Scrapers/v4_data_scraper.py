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
import gender_guesser.detector as gender
from queue import Queue
import heapq

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add a separate handler for debug logging
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

    def can_make_request(self):
        current_time = time.time()
        if current_time - self.hour_start >= 3600:
            self.requests_this_hour = 0
            self.hour_start = current_time
        return self.requests_this_hour < 5

    def increment_request_count(self):
        self.requests_this_hour += 1
        self.last_request_time = time.time()

    def __lt__(self, other):
        return self.last_request_time < other.last_request_time

class InstagramUserDataScraper:
    def __init__(self, user_ids, csv_filename, account_data, db_config):
        self.user_ids = user_ids
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.base_url = "https://i.instagram.com/api/v1/users/{}/info/"
        self.cookie_states = self.initialize_cookie_states()
        self.max_workers = len(self.account_data)
        self.stop_event = threading.Event()
        self.cookie_state_lock = threading.Lock()
        self.gender_detector = gender.Detector()
        self.max_retries = 3
        self.use_proxies = True
        self.account_id_to_index = {account['id']: i for i, account in enumerate(self.account_data)}
        self.index_to_account_id = {i: account['id'] for i, account in enumerate(self.account_data)}
        self.available_cookies = Queue()
        self.initialize_available_cookies()
        self.rate_limit_info = {}
        self.rate_limit_counts = {}
        self.rate_limit_threshold = 3
        self.rate_limit_window = 600
        self.request_lock = threading.Lock()
        self.scraped_users = set()

    def initialize_cookie_states(self):
        cookie_states = []
        for i, account in enumerate(self.account_data):
            proxy = f"{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}"
            cookie_state = CookieState(account['cookies'], proxy, account['user_agent'], i)
            cookie_states.append(cookie_state)
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
                wait_time = 300 - (current_time - cookie_state.last_request_time)
                available_cookies.append((wait_time, current_time, cookie_state))

        if not available_cookies:
            return None

        heapq.heapify(available_cookies)
        next_available = heapq.heappop(available_cookies)
        wait_time, _, cookie_state = next_available

        if wait_time > 0:
            logger.info(f"Waiting {wait_time:.2f} seconds before next request")
            time.sleep(wait_time)

        return cookie_state

    def return_cookie_to_pool(self, cookie_state):
        self.available_cookies.put(cookie_state)

    def scrape_user_data(self):
        logger.info("Starting user data scraping")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.scrape_with_cookie, self.get_next_available_cookie()) 
                       for _ in range(self.max_workers)]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in scrape_user_data: {str(e)}")
                    logger.error(traceback.format_exc())

        logger.info("User data scraping complete.")

    def scrape_with_cookie(self, cookie_state):
        while self.user_ids and not self.stop_event.is_set():
            user_id = self.user_ids.pop(0)
            if user_id in self.scraped_users:
                continue
            
            try:
                user_data = self.fetch_user_data(cookie_state, user_id)
                if user_data == "RATE_LIMITED":
                    self.user_ids.append(user_id)  # Put the user_id back in the queue
                    self.return_cookie_to_pool(cookie_state)
                    cookie_state = self.get_next_available_cookie()
                    if cookie_state is None:
                        logger.warning("No available cookies. Waiting before retry...")
                        time.sleep(300)
                    continue
                elif user_data:
                    self.process_user_data(user_data, user_id)
                    self.scraped_users.add(user_id)
                else:
                    logger.warning(f"Failed to fetch data for user ID: {user_id}")
            except Exception as e:
                logger.error(f"Error processing user ID {user_id}: {str(e)}")
                logger.error(traceback.format_exc())

            self.return_cookie_to_pool(cookie_state)
            cookie_state = self.get_next_available_cookie()

    def fetch_user_data(self, cookie_state, user_id):
        current_account_id = self.index_to_account_id[cookie_state.index]
        url = self.base_url.format(user_id)
        
        headers = {
            'User-Agent': cookie_state.user_agent,
            'Accept-Language': 'en-US',
            'Accept-Encoding': 'gzip, deflate',
            'X-IG-Capabilities': '3brTvw==',
            'X-IG-Connection-Type': 'WIFI',
            'X-IG-App-ID': '567067343352427',
        }

        cookies = dict(cookie.split('=', 1) for cookie in cookie_state.cookie.split('; '))

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

        for retry in range(self.max_retries):
            if not cookie_state.can_make_request():
                logger.debug(f"Rate limit reached for account ID {current_account_id}, signaling to switch cookie...")
                return "RATE_LIMITED"

            try:
                response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies, timeout=30)
                cookie_state.increment_request_count()
                
                self.update_rate_limit_info(current_account_id, response.headers)
                
                response.raise_for_status()
                data = response.json()
                
                if 'user' not in data:
                    logger.warning("'user' key not found in response data. Possible rate limit or API issue.")
                    self.increment_rate_limit_count(current_account_id)
                    return "RATE_LIMITED"
                
                return data
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Request Exception for account ID {current_account_id}, user ID {user_id}: {e}")
            
            logger.debug(f"Retrying in 5 seconds...")
            time.sleep(5)
        
        logger.info(f"Max retries reached for account ID {current_account_id}")
        return None

    def process_user_data(self, user_data, user_id):
        parsed_data = self.parse_user_info(user_data)
        parsed_data['gender'] = self.guess_gender(parsed_data['full_name'])
        parsed_data['user_id'] = user_id
        parsed_data['csv_filename'] = self.csv_filename
        self.upload_to_database(parsed_data)

    def parse_user_info(self, user_info):
        user = user_info['user']
        return {
            'username': user.get('username'),
            'full_name': user.get('full_name'),
            'follower_count': user.get('follower_count'),
            'following_count': user.get('following_count'),
            'media_count': user.get('media_count'),
            'is_private': user.get('is_private'),
            'is_verified': user.get('is_verified'),
            'biography': user.get('biography'),
            'external_url': user.get('external_url'),
            'profile_pic_url': user.get('profile_pic_url'),
        }

    def guess_gender(self, name):
        if name and ' ' in name:
            first_name = name.split()[0]
        else:
            first_name = name
        return self.gender_detector.get_gender(first_name) if first_name else 'unknown'

    def upload_to_database(self, data):
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            insert_query = """
            INSERT INTO instagram_users (
                user_id, username, full_name, follower_count, following_count, 
                media_count, is_private, is_verified, biography, external_url, 
                profile_pic_url, gender, csv_filename
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                profile_pic_url = VALUES(profile_pic_url),
                gender = VALUES(gender),
                csv_filename = VALUES(csv_filename)
            """

            cursor.execute(insert_query, (
                data['user_id'], data['username'], data['full_name'], data['follower_count'],
                data['following_count'], data['media_count'], data['is_private'], data['is_verified'],
                data['biography'], data['external_url'], data['profile_pic_url'], data['gender'],
                data['csv_filename']
            ))

            connection.commit()
            logger.info(f"Inserted/Updated user data for user ID: {data['user_id']}")

        except Error as e:
            logger.error(f"Error saving user data to database: {e}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def update_rate_limit_info(self, account_id, response_headers):
        if 'X-RateLimit-Remaining' in response_headers:
            remaining = int(response_headers['X-RateLimit-Remaining'])
            reset_time = int(response_headers.get('X-RateLimit-Reset', 0))
            self.rate_limit_info[account_id] = {
                'remaining': remaining,
                'reset_time': reset_time
            }

    def increment_rate_limit_count(self, account_id):
        current_time = time.time()
        self.rate_limit_counts[account_id] = [t for t in self.rate_limit_counts.get(account_id, []) if current_time - t < self.rate_limit_window]
        self.rate_limit_counts[account_id].append(current_time)

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