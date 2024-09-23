import json
from pprint import pprint
import time
import requests
import csv
import random
import logging
import traceback
from mysql.connector import Error
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from datetime import datetime, timedelta
import threading
from mysql.connector.pooling import MySQLConnectionPool

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramTaggedScraper:
    def __init__(self, user_data, target_user_id, target_username, csv_filename, account_data, db_config):
        self.user_data = user_data  # This should be a dict with user_id as key and username as value
        self.target_user_id = str(target_user_id)
        self.target_username = target_username.lower()
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.db_pool = MySQLConnectionPool(pool_name="mypool", pool_size=5, **self.db_config)
        self.account_id_to_index = {}
        self.setup_accounts()
        self.account_timeouts = {}
        self.account_lock = threading.Lock()
        self.processed_users = set()
        self.tagged_posts = {}
        self.last_response_text = ""
        self.account_jitter_info = {}
        self.successful_fetches = 0
        self.user_tried_accounts = {}
        self.successful_taggers = set()

    def setup_accounts(self):
        for i, account in enumerate(self.account_data):
            self.account_id_to_index[account['id']] = i

    def get_next_available_account(self):
        current_time = datetime.now()
        with self.account_lock:
            available_accounts = [account for account in self.account_data 
                                  if account['id'] not in self.account_timeouts or 
                                  current_time > self.account_timeouts[account['id']]]
            
            if not available_accounts:
                if self.account_timeouts:
                    soonest_available = min(self.account_timeouts.values())
                    wait_time = max(0, (soonest_available - current_time).total_seconds())
                    if wait_time > 0:
                        logger.info(f"All accounts on timeout. Waiting {wait_time:.2f} seconds for next available account.")
                        self.display_account_status()
                        time.sleep(wait_time)
                    available_accounts = [account for account in self.account_data 
                                          if account['id'] not in self.account_timeouts or 
                                          current_time > self.account_timeouts[account['id']]]
                
            if available_accounts:
                account = random.choice(available_accounts)
                return account
            
            return None

    def display_account_status(self):
        current_time = datetime.now()
        active_accounts = []
        cooldown_accounts = {}
        
        with self.account_lock:
            for account in self.account_data:
                account_id = account['id']
                if account_id not in self.account_timeouts or current_time > self.account_timeouts[account_id]:
                    active_accounts.append(account_id)
                else:
                    cooldown_accounts[account_id] = self.account_timeouts[account_id]
        
        logger.info("Account Status:")
        logger.info(f"Active accounts: {', '.join(map(str, active_accounts))}")
        
        if active_accounts:
            logger.info("Active account details:")
            for account_id in active_accounts:
                if account_id in self.account_jitter_info:
                    last_jitter_wait, last_jitter_time = self.account_jitter_info[account_id]
                    remaining_wait = max(0, last_jitter_wait - (time.time() - last_jitter_time))
                    logger.info(f"  Account {account_id}: Jitter wait remaining: {remaining_wait:.2f} seconds")
                else:
                    logger.info(f"  Account {account_id}: No current jitter wait")
        
        if cooldown_accounts:
            logger.info("Accounts in cooldown:")
            for account_id, timeout in cooldown_accounts.items():
                remaining_time = max(0, (timeout - current_time).total_seconds())
                logger.info(f"  Account {account_id}: {remaining_time:.2f} seconds remaining")
        else:
            logger.info("No accounts in cooldown")

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
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return None

    def check_and_update_cookie(self, account):
        account_id = account['id']
        logger.debug(f"Checking for new cookie for account ID {account_id}")
        new_cookie = self.get_new_cookie_from_db(account_id, account['cookies'])
        if new_cookie and new_cookie != account['cookies']:
            logger.info(f"New cookie found for account ID {account_id}. Updating.")
            logger.info(f'New cookie: {new_cookie}')
            logger.info(f'Current cookie: {account["cookies"]}')
            account['cookies'] = new_cookie
        else:
            logger.debug(f"No new cookie found for account ID {account_id}")
        return account

    def fetch_user_posts(self, user_id, account):
        url = f"https://i.instagram.com/api/v1/feed/user/{user_id}/"
        headers = {
            'User-Agent': account['user_agent'],
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'X-IG-App-ID': '936619743392459',
            'X-ASBD-ID': '198387',
            'X-IG-WWW-Claim': '0',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
            'Referer': 'https://www.instagram.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
        }
        cookies = {cookie.split('=')[0]: cookie.split('=')[1] for cookie in account['cookies'].split('; ')}
        proxies = {
            'http': f"http://{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}",
            'https': f"http://{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}"
        }

        try:
            response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
            if response.status_code == 200:
                data = response.json()
                # print(f'Posts for user {user_id}:')
                # pprint(data)
                return data['items']  # Return full post data
            else:
                logger.warning(f"Failed to fetch posts for user {user_id}. Status code: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error occurred while fetching posts for user {user_id}: {str(e)}")
            return None

    def check_post_for_tag_in_post_data(self, post):
        # Check 'usertags'
        usertags = post.get('usertags', {}).get('in', [])
        for tag in usertags:
            tagged_user_id = str(tag['user']['pk'])
            if tagged_user_id == self.target_user_id:
                return True

        # Check 'coauthor_producers' and 'invited_coauthor_producers'
        coauthors = post.get('coauthor_producers', []) + post.get('invited_coauthor_producers', [])
        for coauthor in coauthors:
            coauthor_id = str(coauthor['id'])
            if coauthor_id == self.target_user_id:
                return True

        # Check mentions in 'caption'
        caption_text = post.get('caption', {}).get('text', '')
        if f"@{self.target_username}" in caption_text.lower():
            return True

        return False

    def process_single_user(self, user_id):
        username = self.user_data.get(user_id, "Unknown")
        logger.info(f"Processing user ID {user_id} (Username: {username})")
        
        if user_id in self.processed_users:
            logger.info(f"User ID {user_id} (Username: {username}) already processed. Skipping.")
            return

        account = self.get_next_available_account()
        if not account:
            logger.error(f"No available accounts to process user ID {user_id} (Username: {username})")
            return

        max_retries = 5
        retries = 0
        self.user_tried_accounts[user_id] = set()

        while retries < max_retries:
            if account['id'] in self.user_tried_accounts[user_id]:
                logger.info(f"Account {account['id']} already tried for user ID {user_id} (Username: {username}), looking for another account")
                account = self.get_next_available_account()
                if not account:
                    logger.error(f"No available accounts to process user ID {user_id} (Username: {username})")
                    return

            try:
                self.display_account_status()
                posts = self.fetch_user_posts(user_id, account)
                if posts is None:
                    logger.warning(f"Failed to fetch posts for user ID {user_id} (Username: {username})")
                    self.set_account_timeout(account['id'])
                else:
                    tagged_posts = []
                    for post in posts:
                        if self.check_post_for_tag_in_post_data(post):
                            tagged_posts.append(post['id'])
                    
                    if tagged_posts:
                        self.tagged_posts[user_id] = tagged_posts
                        self.successful_taggers.add(user_id)
                        logger.info(f"User ID {user_id} (Username: {username}) has {len(tagged_posts)} posts tagging the target user")
                    else:
                        logger.info(f"User ID {user_id} (Username: {username}) has no posts tagging the target user")
                    
                    self.processed_users.add(user_id)
                    with self.account_lock:
                        self.successful_fetches += 1
                    logger.info(f"Successfully processed user ID {user_id} (Username: {username})")
                    self.wait_with_jitter(account['id'])
                    return
            except Exception as e:
                logger.error(f"Error occurred while scraping user ID {user_id} (Username: {username}): {str(e)}")
                logger.error(traceback.format_exc())
                self.set_account_timeout(account['id'])

            self.wait_with_jitter(account['id'])
            retries += 1

        logger.error(f"Max retries reached for user ID {user_id} (Username: {username}). Skipping.")

    def set_account_timeout(self, account_id):
        timeout_until = datetime.now() + timedelta(minutes=5)
        with self.account_lock:
            self.account_timeouts[account_id] = timeout_until
        logger.info(f"Account {account_id} set on timeout until {timeout_until}")
        self.display_account_status()

    def wait_with_jitter(self, account_id):
        activity_type = random.choices(['quick', 'normal', 'engaged'], weights=[0.3, 0.5, 0.2])[0]
        
        if activity_type == 'quick':
            jitter = np.random.exponential(scale=2)
        elif activity_type == 'normal':
            jitter = np.random.normal(loc=10, scale=5)
        else:  # engaged
            jitter = np.random.normal(loc=30, scale=10)

        if random.random() < 0.1:
            jitter += np.random.uniform(60, 300)

        jitter = max(jitter, 15)

        logger.info(f"Waiting for {jitter:.2f} seconds for account {account_id}.")
        self.account_jitter_info[account_id] = (jitter, time.time())
        time.sleep(jitter)

    def scrape_tagged_posts(self):
        start_time = time.time()
        total_users = len(self.user_data)
        processed_count = 0

        with ThreadPoolExecutor(max_workers=len(self.account_data)) as executor:
            futures = [executor.submit(self.process_single_user, user_id) for user_id in self.user_data.keys()]
            
            total_to_process = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                future.result()
                processed_count += 1
                
                if i % 2 == 0 or i == total_to_process:
                    self.display_account_status()
                    self.display_progress(processed_count, total_users, start_time)

        self.save_results()
        logger.info(f"Scraping completed. Processed {len(self.processed_users)} user IDs.")
        self.display_tagging_summary()

    def display_progress(self, processed_count, total_users, start_time):
        elapsed_time = time.time() - start_time
        progress_percentage = (processed_count / total_users) * 100
        
        logger.info(f"Progress: {processed_count}/{total_users} ({progress_percentage:.2f}%)")

        if elapsed_time > 0:
            overall_rate = self.successful_fetches / elapsed_time
            logger.info(f"Overall processing rate: {overall_rate:.2f} successful users/second")
            logger.info(f"Total successful fetches: {self.successful_fetches}")
            
            if overall_rate > 0:
                remaining_users = total_users - self.successful_fetches
                estimated_time_remaining = remaining_users / overall_rate
                logger.info(f"Estimated time remaining: {timedelta(seconds=int(estimated_time_remaining))}")
            else:
                logger.info("Estimated time remaining: Unable to calculate (processing rate is 0)")
        else:
            logger.info("Estimated time remaining: Calculating...")

        unsuccessful_attempts = processed_count - self.successful_fetches
        if unsuccessful_attempts > 0:
            unsuccessful_rate = unsuccessful_attempts / elapsed_time
            logger.info(f"Unsuccessful attempts: {unsuccessful_attempts}")
            logger.info(f"Unsuccessful rate: {unsuccessful_rate:.2f} users/second")

    def create_tagged_posts_table(self, connection):
        cursor = None
        try:
            cursor = connection.cursor()
            create_table_query = """
            CREATE TABLE IF NOT EXISTS tagged_posts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255),
                post_id VARCHAR(255),
                target_user_id VARCHAR(255),
                csv_filename VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_tag (user_id, post_id, target_user_id)
            )
            """
            cursor.execute(create_table_query)
            connection.commit()
            logger.info("Tagged posts table created or already exists")
        except Error as e:
            logger.error(f"Error creating tagged_posts table: {e}")
        finally:
            if cursor:
                cursor.close()

    def save_results(self):
        connection = None
        cursor = None
        try:
            connection = self.db_pool.get_connection()
            
            # Create the table if it doesn't exist
            self.create_tagged_posts_table(connection)
            
            cursor = connection.cursor()

            query = """
            INSERT INTO tagged_posts (user_id, post_id, target_user_id, csv_filename)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE post_id = VALUES(post_id)
            """

            for user_id, posts in self.tagged_posts.items():
                for post_id in posts:
                    cursor.execute(query, (user_id, post_id, self.target_user_id, self.csv_filename))

            connection.commit()
            logger.info(f"Saved {sum(len(posts) for posts in self.tagged_posts.values())} tagged posts to database")

        except Error as e:
            logger.error(f"Error saving results to database: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        with open(self.csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['User ID', 'Post ID', 'Target User ID'])
            for user_id, posts in self.tagged_posts.items():
                for post_id in posts:
                    writer.writerow([user_id, post_id, self.target_user_id])
        logger.info(f"Saved results to CSV file: {self.csv_filename}")

    def display_tagging_summary(self):
        logger.info("=== Tagging Summary ===")
        logger.info(f"Total users who tagged the target: {len(self.successful_taggers)}")
        logger.info("Users who tagged the target:")
        for user_id in self.successful_taggers:
            username = self.user_data.get(user_id, "Unknown")
            post_count = len(self.tagged_posts.get(user_id, []))
            logger.info(f"  - User ID: {user_id}, Username: {username}, Tagged Posts: {post_count}")

def main():
    import sys
    if len(sys.argv) != 7:
        print("Usage: python tagged_scraper.py <user_data_json> <target_user_id> <target_username> <csv_filename> <account_data_json> <db_config_json>")
        sys.exit(1)

    user_data = json.loads(sys.argv[1])  # Dict with user_id as key and username as value
    target_user_id = sys.argv[2]
    target_username = sys.argv[3]
    csv_filename = sys.argv[4]
    account_data = sys.argv[5]
    db_config = sys.argv[6]

    scraper = InstagramTaggedScraper(user_data, target_user_id, target_username, csv_filename, account_data, db_config)
    logger.info(f"Scraping tagged posts for {len(user_data)} users")
    logger.info(f"Using accounts with IDs: {', '.join(str(id) for id in scraper.account_id_to_index.keys())}")
    scraper.display_account_status()
    scraper.scrape_tagged_posts()
    logger.info(f"Number of successful taggers: {len(scraper.successful_taggers)}")

if __name__ == "__main__":
    main()