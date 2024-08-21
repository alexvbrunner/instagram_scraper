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
from queue import Queue, PriorityQueue
import threading
from datetime import datetime, timedelta
from collections import deque
from mysql.connector.pooling import MySQLConnectionPool


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CookieState:
    def __init__(self, cookie, proxy, user_agent, index, account_id):
        self.cookie = cookie
        self.proxy = proxy
        self.user_agent = user_agent
        self.index = index
        self.account_id = account_id
        self.active = True
        self.last_request_time = 0
        self.fail_count = 0
        self.requests_this_hour = 0
        self.hour_start = time.time()
        self.last_cookie_check = time.time()
        self.is_rate_limited = False
        self.cooldown_time = 10
        self.min_cooldown = 0.1
        self.max_requests_per_hour = 300
        self.rate_limit_start_time = 0
        self.requests_per_minute = 30  # Adjust as needed
        self.request_times = deque(maxlen=self.requests_per_minute)
        self.rate_limit_until = 0
        self.lock = threading.Lock()
        self.in_use = False

    def can_make_request(self):
        current_time = time.time()
        while self.request_times and current_time - self.request_times[0] > 60:
            self.request_times.popleft()
        return len(self.request_times) < self.requests_per_minute

    def record_request(self):
        self.request_times.append(time.time())

    def increment_request_count(self):
        self.requests_this_hour += 1
        self.last_request_time = time.time()

    def __lt__(self, other):
        return self.last_request_time < other.last_request_time

    def __eq__(self, other):
        return self.last_request_time == other.last_request_time

    def check_and_reset_rate_limit(self):
        current_time = time.time()
        if self.is_rate_limited and current_time - self.rate_limit_start_time >= self.cooldown_time:
            self.is_rate_limited = False
            self.cooldown_time = self.min_cooldown
            logger.info(f"Rate limit cooldown completed for account ID {self.account_id}. Account is now available.")

    def set_rate_limit(self):
        self.is_rate_limited = True
        self.rate_limit_start_time = time.time()
        self.cooldown_time = max(self.min_cooldown, 300)  # 5 minutes
        self.rate_limit_until = time.time() + self.cooldown_time
        logger.warning(f"Rate limit set for account ID {self.account_id}. Cooldown: {self.cooldown_time} seconds")

    def time_until_available(self):
        current_time = time.time()
        if self.is_rate_limited:
            return max(0, self.rate_limit_until - current_time)
        else:
            return max(0, self.last_request_time + self.cooldown_time - current_time)

    def is_rate_limit_expired(self):
        current_time = time.time()
        return self.is_rate_limited and current_time - self.rate_limit_start_time >= self.cooldown_time

    def reset_rate_limit(self):
        self.is_rate_limited = False
        self.cooldown_time = self.min_cooldown
        logger.info(f"Rate limit reset for account ID {self.account_id}")

    def is_available(self):
        current_time = time.time()
        return (not self.is_rate_limited and 
                current_time >= self.rate_limit_until and 
                current_time - self.last_request_time >= self.cooldown_time)

    def acquire(self):
        with self.lock:
            if self.in_use or not self.is_available():
                return False
            self.in_use = True
            return True

    def release(self):
        with self.lock:
            self.in_use = False

class InstagramUserDataScraper:
    def __init__(self, user_ids, csv_filename, account_data, db_config):
        self.user_ids = user_ids
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.account_id_to_index = {account['id']: index for index, account in enumerate(self.account_data)}
        self.index_to_account_id = {index: account['id'] for index, account in enumerate(self.account_data)}
        self.rate_limit_info = {account['id']: {'remaining': 200, 'reset_time': 0} for account in self.account_data}
        self.gender_detector = gender.Detector()
        self.state = self.load_state()
        self.cookie_states = self.initialize_cookie_states()
        self.account_queue = PriorityQueue()
        self.initialize_account_queue()
        self.account_lock = threading.Lock()
        self.max_concurrent_requests = min(len(self.cookie_states), 10)  # Adjust the maximum as needed
        self.request_lock = threading.Lock()
        self.account_wait_times = {account['id']: 0 for account in self.account_data}
        self.start_time = time.time()
        self.scrape_count = 0
        self.user_queue = Queue()
        for user_id in self.user_ids:
            self.user_queue.put(user_id)
        self.processing_users = set()
        self.processing_lock = threading.Lock()
        self.scrape_times = deque(maxlen=100)  # Store the last 100 scrape times
        self.last_scrape_time = None
        self.session_scrape_count = 0  # New counter for this session's scrapes
        self.db_pool = MySQLConnectionPool(pool_name="mypool", pool_size=self.max_concurrent_requests, **self.db_config)

    def initialize_cookie_states(self):
        cookie_states = []
        for i, account in enumerate(self.account_data):
            proxy = f"{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}"
            cookie_state = CookieState(account['cookies'], proxy, account['user_agent'], i, account['id'])
            cookie_states.append(cookie_state)
        return cookie_states

    def initialize_account_queue(self):
        current_time = time.time()
        for cookie_state in self.cookie_states:
            self.account_queue.put((current_time, cookie_state))

    def get_next_available_account(self):
        with self.account_lock:
            current_time = time.time()
            for _ in range(len(self.cookie_states)):
                if self.account_queue.empty():
                    self.initialize_account_queue()
                    continue

                next_available_time, cookie_state = self.account_queue.get()

                if current_time >= next_available_time and cookie_state.is_available() and cookie_state.acquire():
                    logger.info(f"Using account ID {cookie_state.account_id}")
                    return cookie_state
                else:
                    # Put the account back in the queue
                    self.account_queue.put((max(next_available_time, cookie_state.rate_limit_until), cookie_state))

        logger.warning("No available accounts at the moment.")
        return None

    def return_account_to_queue(self, cookie_state, cooldown=0):
        with self.account_lock:
            cookie_state.release()
            next_available_time = max(time.time() + cooldown, cookie_state.rate_limit_until)
            self.account_queue.put((next_available_time, cookie_state))
            self.account_wait_times[cookie_state.account_id] = max(cooldown, cookie_state.cooldown_time)
            logger.info(f"Account ID {cookie_state.account_id} returned to queue. Next available at: {next_available_time}")

    def load_state(self):
        try:
            with open(f'Files/States/{self.csv_filename}_state.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {
                'scraped_users': [],
                'total_scraped': 0,
                'skipped_user_ids': []
            }

    def save_state(self):
        with open(f'Files/States/{self.csv_filename}_state.json', 'w') as f:
            json.dump(self.state, f)

    def record_scrape(self):
        current_time = time.time()
        if self.last_scrape_time is not None:
            self.scrape_times.append(current_time - self.last_scrape_time)
        self.last_scrape_time = current_time
        self.session_scrape_count += 1  # Increment the session scrape count
        logger.debug(f"Recorded scrape. Session scrape count: {self.session_scrape_count}")

    def get_average_scrape_rate(self):
        if not self.scrape_times:
            return 0
        average_time_per_scrape = sum(self.scrape_times) / len(self.scrape_times)
        return 60 / average_time_per_scrape  # scrapes per minute

    def display_statistics(self):
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        total_users = len(self.user_ids)
        processed_users = len(self.state['scraped_users']) + len(self.state['skipped_user_ids'])

        # Calculate scrape rates
        scrapes_per_minute = self.get_average_scrape_rate()
        scrapes_per_hour = scrapes_per_minute * 60
        scrapes_per_day = scrapes_per_hour * 24

        # Calculate available and timeout accounts
        available_accounts = []
        rate_limited_accounts = []
        for cs in self.cookie_states:
            account_id = self.index_to_account_id[cs.index]
            time_until_available = cs.time_until_available()
            if cs.active and not cs.is_rate_limited:
                available_accounts.append((account_id, time_until_available))
            else:
                rate_limited_accounts.append((account_id, time_until_available))

        # Estimate time to completion
        remaining_users = total_users - processed_users
        if scrapes_per_minute > 0:
            estimated_completion_time = remaining_users / scrapes_per_minute
            completion_time = timedelta(minutes=estimated_completion_time)
        else:
            completion_time = "Unable to estimate"

        logger.info("----- Scraping Statistics -----")
        logger.info(f"Progress: {processed_users}/{total_users} users processed")
        logger.info(f"Scrapes this session: {self.session_scrape_count}")
        logger.debug(f"Debug: session_scrape_count = {self.session_scrape_count}")
        logger.info(f"Elapsed time: {timedelta(seconds=int(elapsed_time))}")
        logger.info(f"Average scrapes per minute: {scrapes_per_minute:.2f}")
        logger.info(f"Average scrapes per hour: {scrapes_per_hour:.2f}")
        logger.info(f"Average scrapes per day: {scrapes_per_day:.2f}")
        logger.info("Account status:")
        for account_id, wait_time in available_accounts:
            logger.info(f"  Account ID {account_id}: Available (wait time: {wait_time:.2f} seconds)")
        for account_id, wait_time in rate_limited_accounts:
            logger.info(f"  Account ID {account_id}: Rate-limited (wait time: {wait_time:.2f} seconds)")
        logger.info(f"Estimated time to completion: {completion_time}")
        logger.info(f"Currently processing users: {len(self.processing_users)}")
        logger.info(f"Remaining users in queue: {self.user_queue.qsize()}")
        logger.info("-------------------------------")

    def scrape_user_data(self):
        total_users = len(self.user_ids)
        last_stats_time = time.time()
        stats_interval = 5  # Display statistics every 5 seconds

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
            futures = set()
            while not self.user_queue.empty() or futures:
                while len(futures) < self.max_concurrent_requests and not self.user_queue.empty():
                    user_id = self.user_queue.get()
                    futures.add(executor.submit(self.process_single_user, user_id))

                # Wait for any future to complete
                done, futures = concurrent.futures.wait(
                    futures, 
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=1  # Add a timeout to prevent blocking
                )

                for future in done:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error in thread: {str(e)}")

                current_time = time.time()
                if current_time - last_stats_time >= stats_interval:
                    self.display_statistics()
                    last_stats_time = current_time

        self.display_statistics()  # Display final statistics
        logger.info("User data scraping process completed for all user IDs.")
        logger.info(f"Final total completed user data scrapes: {len(self.state['scraped_users'])}")
        logger.info(f"Total skipped user IDs: {len(self.state['skipped_user_ids'])}")

    def process_single_user(self, user_id):
        logger.info(f"Processing user ID {user_id}")
        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            account = self.get_next_available_account()
            if account is None:
                logger.warning(f"No available accounts. Waiting before retry for user ID {user_id}")
                self.display_statistics()
                time.sleep(30)  # Wait for 30 seconds before trying again
                continue

            try:
                logger.info(f"Using account ID {account.account_id} for user ID {user_id}")
                user_data = self.fetch_user_data(user_id, account)
                logger.debug(f"User data: {user_data}")
                logger.debug(f"User data type: {type(user_data)}")
                if user_data:
                    processed_data = self.process_user_data(user_data)
                    if processed_data:
                        self.save_user_data(processed_data)
                        with self.processing_lock:
                            self.state['scraped_users'].append(user_id)
                            self.state['total_scraped'] += 1
                            self.processing_users.discard(user_id)  # Use discard instead of remove
                            self.record_scrape()  # Record the successful scrape
                        logger.info(f"Successfully scraped data for user ID: {user_id}")
                        logger.info(f"Session scrape count: {self.session_scrape_count}")  # Log the current count
                        break
                    else:
                        logger.error(f"Failed to process data for user ID: {user_id}")
                        with self.processing_lock:
                            self.state['skipped_user_ids'].append(user_id)
                            self.processing_users.discard(user_id)  # Use discard instead of remove
                        break
                else:
                    logger.warning(f"No data found for user ID: {user_id}")
                    retry_count += 1
                    account.set_rate_limit()
            except Exception as e:
                logger.error(f"Error occurred while scraping User ID {user_id}: {str(e)}")
                logger.error(traceback.format_exc())
                retry_count += 1
                account.set_rate_limit()
            finally:
                self.return_account_to_queue(account)

            if retry_count < max_retries:
                logger.info(f"Retrying user ID {user_id} (Attempt {retry_count + 1}/{max_retries})")
            else:
                logger.warning(f"Max retries reached for user ID: {user_id}")
                with self.processing_lock:
                    self.state['skipped_user_ids'].append(user_id)
                    self.processing_users.discard(user_id)  # Use discard instead of remove

        self.save_state()

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

    def check_and_update_cookie(self, cookie_state):
        account_id = cookie_state.account_id
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

    def fetch_user_data(self, user_id, account):
        if not account.can_make_request():
            logger.warning(f"Rate limit reached for account ID {account.account_id}")
            account.set_rate_limit()
            return None

        with self.request_lock:
            cookie_state = account
            if cookie_state is None:
                logger.warning("No available cookies. Waiting before retry...")
                time.sleep(60)
                return None

        current_account_id = cookie_state.account_id
        logger.info(f"Fetching data for user ID {user_id} with account ID {current_account_id}")
        
        cookie_state = self.check_and_update_cookie(cookie_state)
        
        url = f"https://i.instagram.com/api/v1/users/{user_id}/info/"
        
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

        proxies = {
            'http': f'http://{cookie_state.proxy}',
            'https': f'http://{cookie_state.proxy}'
        }

        try:
            response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies, timeout=30)
            account.record_request()
            response.raise_for_status()
            data = response.json()
                
            logger.debug(f"Successfully fetched data")
            logger.info(f"Response data: {json.dumps(data, indent=2)}")
            
            cookie_state.increment_request_count()
            
            if 'user' in data:
                return data['user']
            else:
                logger.warning(f"No user data found for user ID: {user_id}")
                return None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                response_content = e.response.content.decode('utf-8')
                if "challenge_required" in response_content:
                    logger.error(f"Challenge required for account ID {current_account_id}. Disabling this account for the session.")
                    cookie_state.active = False
                    self.save_state()
                    return None
                else:
                    logger.error(f"HTTP Error 400 for account ID {current_account_id}, user ID {user_id}: {e}")
                    logger.error(f"Response content: {response_content}")
                    return None
            elif e.response.status_code in [401, 429]:
                logger.warning(f"Rate limit hit for account ID {current_account_id}.")
                account.set_rate_limit()
                return None
            else:
                logger.error(f"HTTP Error for account ID {current_account_id}, user ID {user_id}: {e}")
                logger.error(f"Response content: {e.response.content}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for user ID {user_id}: {str(e)}")
        
        return None

    def update_rate_limit_info(self, account_id, headers):
        if 'X-Ratelimit-Remaining' in headers:
            self.rate_limit_info[account_id]['remaining'] = int(headers['X-Ratelimit-Remaining'])
        if 'X-Ratelimit-Reset' in headers:
            self.rate_limit_info[account_id]['reset_time'] = time.time() + int(headers['X-Ratelimit-Reset'])

    def process_user_data(self, user_data):
        if user_data:
            parsed_data = {
                'user_id': user_data.get('pk'),
                'username': user_data.get('username'),
                'full_name': user_data.get('full_name'),
                'biography': user_data.get('biography'),
                'follower_count': user_data.get('follower_count'),
                'following_count': user_data.get('following_count'),
                'media_count': user_data.get('media_count'),
                'is_private': user_data.get('is_private'),
                'is_verified': user_data.get('is_verified'),
                'category': user_data.get('category'),
                'external_url': user_data.get('external_url'),
                'public_email': user_data.get('public_email'),
                'public_phone_number': user_data.get('public_phone_number'),
                'is_business': user_data.get('is_business'),
                'profile_pic_url': user_data.get('profile_pic_url'),
                'hd_profile_pic_url': user_data.get('hd_profile_pic_url_info', {}).get('url'),
                'has_highlight_reels': user_data.get('has_highlight_reels'),
                'has_guides': user_data.get('has_guides'),
                'is_interest_account': user_data.get('is_interest_account'),
                'total_igtv_videos': user_data.get('total_igtv_videos'),
                'total_clips_count': user_data.get('total_clips_count', 0),
                'total_ar_effects': user_data.get('total_ar_effects'),
                'is_eligible_for_smb_support_flow': user_data.get('is_eligible_for_smb_support_flow'),
                'is_eligible_for_lead_center': user_data.get('is_eligible_for_lead_center'),
                'account_type': user_data.get('account_type'),
                'is_call_to_action_enabled': user_data.get('is_call_to_action_enabled'),
                'interop_messaging_user_fbid': user_data.get('interop_messaging_user_fbid'),
                'has_videos': user_data.get('has_videos'),
                'total_video_count': user_data.get('total_video_count', 0),
                'has_music_on_profile': user_data.get('has_music_on_profile'),
                'is_potential_business': user_data.get('is_potential_business'),
                'is_memorialized': user_data.get('is_memorialized'),
                'gender': self.guess_gender(user_data.get('full_name')),
                'csv_filename': self.csv_filename,
                'bio_links': [link.get('url') for link in user_data.get('bio_links', [])],
                'pinned_channels_info': user_data.get('pinned_channels_info', {})
            }
            return parsed_data
        else:
            logger.warning(f"No user data found for user ID: {user_data.get('pk')}")
            return None

    def guess_gender(self, name):
        if name and ' ' in name:
            first_name = name.split()[0]
        else:
            first_name = name
        return self.gender_detector.get_gender(first_name) if first_name else 'unknown'

    def get_db_connection(self):
        return self.db_pool.get_connection()

    def save_user_data(self, user_data):
        if not user_data:
            logger.error(f"No data to save for user ID: {user_data.get('user_id')}")
            return

        try:
            connection = self.get_db_connection()
            cursor = connection.cursor()

            # Check if user_id column exists
            cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'users'
            AND COLUMN_NAME = 'user_id'
            """)
            user_id_exists = cursor.fetchone()[0]

            if not user_id_exists:
                # Add user_id column if it doesn't exist
                cursor.execute("""
                ALTER TABLE users
                ADD COLUMN user_id BIGINT UNIQUE
                """)
                logger.info("Added user_id column to users table")

            # Insert data into the users table
            insert_query = """
            INSERT INTO users (
                user_id, username, full_name, biography, follower_count, following_count,
                media_count, is_private, is_verified, category, external_url,
                public_email, public_phone_number, is_business, profile_pic_url,
                hd_profile_pic_url, has_highlight_reels, has_guides,
                is_interest_account, total_igtv_videos, total_clips_count,
                total_ar_effects, is_eligible_for_smb_support_flow,
                is_eligible_for_lead_center, account_type, is_call_to_action_enabled,
                interop_messaging_user_fbid, has_videos, total_video_count,
                has_music_on_profile, is_potential_business, is_memorialized, gender,
                csv_filename
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                username = VALUES(username),
                full_name = VALUES(full_name),
                biography = VALUES(biography),
                follower_count = VALUES(follower_count),
                following_count = VALUES(following_count),
                media_count = VALUES(media_count),
                is_private = VALUES(is_private),
                is_verified = VALUES(is_verified),
                category = VALUES(category),
                external_url = VALUES(external_url),
                public_email = VALUES(public_email),
                public_phone_number = VALUES(public_phone_number),
                is_business = VALUES(is_business),
                profile_pic_url = VALUES(profile_pic_url),
                hd_profile_pic_url = VALUES(hd_profile_pic_url),
                has_highlight_reels = VALUES(has_highlight_reels),
                has_guides = VALUES(has_guides),
                is_interest_account = VALUES(is_interest_account),
                total_igtv_videos = VALUES(total_igtv_videos),
                total_clips_count = VALUES(total_clips_count),
                total_ar_effects = VALUES(total_ar_effects),
                is_eligible_for_smb_support_flow = VALUES(is_eligible_for_smb_support_flow),
                is_eligible_for_lead_center = VALUES(is_eligible_for_lead_center),
                account_type = VALUES(account_type),
                is_call_to_action_enabled = VALUES(is_call_to_action_enabled),
                interop_messaging_user_fbid = VALUES(interop_messaging_user_fbid),
                has_videos = VALUES(has_videos),
                total_video_count = VALUES(total_video_count),
                has_music_on_profile = VALUES(has_music_on_profile),
                is_potential_business = VALUES(is_potential_business),
                is_memorialized = VALUES(is_memorialized),
                gender = VALUES(gender),
                csv_filename = VALUES(csv_filename)
            """

            user_data_tuple = tuple(user_data.get(field) for field in [
                'user_id', 'username', 'full_name', 'biography', 'follower_count', 'following_count',
                'media_count', 'is_private', 'is_verified', 'category', 'external_url',
                'public_email', 'public_phone_number', 'is_business', 'profile_pic_url',
                'hd_profile_pic_url', 'has_highlight_reels', 'has_guides',
                'is_interest_account', 'total_igtv_videos', 'total_clips_count',
                'total_ar_effects', 'is_eligible_for_smb_support_flow',
                'is_eligible_for_lead_center', 'account_type', 'is_call_to_action_enabled',
                'interop_messaging_user_fbid', 'has_videos', 'total_video_count',
                'has_music_on_profile', 'is_potential_business', 'is_memorialized', 'gender',
                'csv_filename'
            ])

            cursor.execute(insert_query, user_data_tuple)

            # Insert bio links
            if 'bio_links' in user_data:
                for link in user_data['bio_links']:
                    insert_link_query = """
                    INSERT INTO bio_links (user_id, url)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE url = VALUES(url)
                    """
                    cursor.execute(insert_link_query, (user_data['user_id'], link))

            # Insert pinned channels
            if 'pinned_channels_info' in user_data and 'pinned_channels_list' in user_data['pinned_channels_info']:
                for channel in user_data['pinned_channels_info']['pinned_channels_list']:
                    insert_channel_query = """
                    INSERT INTO pinned_channels (user_id, title, subtitle, invite_link, number_of_members)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        subtitle = VALUES(subtitle),
                        invite_link = VALUES(invite_link),
                        number_of_members = VALUES(number_of_members)
                    """
                    channel_data = (
                        user_data['user_id'], channel.get('title'), channel.get('subtitle'),
                        channel.get('invite_link'), channel.get('number_of_members')
                    )
                    cursor.execute(insert_channel_query, channel_data)

            connection.commit()
            logger.info(f"Saved/Updated user data for user ID: {user_data['user_id']}")

        except Error as e:
            logger.error(f"Error saving user data to database: {e}")
            logger.error(f"User data: {user_data}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

def main():
    import sys
    if len(sys.argv) != 5:
        print("Usage: python v4_data_scraper.py <user_ids_json> <csv_filename> <account_data_json> <db_config_json>")
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