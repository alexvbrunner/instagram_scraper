import json
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
from db_utils import (
    get_database_connection,
    get_accounts_from_database,
    prepare_account_data,
    update_account_last_checked,
    mark_account_invalid
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramUserIDScraper:
    def __init__(self, usernames, csv_filename, account_data, db_config):
        self.usernames = usernames
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.db_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **self.db_config)
        self.account_id_to_index = {}
        self.setup_accounts()
        self.processed_usernames = set()
        self.existing_user_ids = self.load_existing_user_ids()
        self.account_timeouts = {}
        self.account_lock = threading.Lock()
        self.last_response_text = ""
        self.account_jitter_info = {}  # New dictionary to store jitter info per account
        self.successful_fetches = 0  # Add this line to initialize the counter
        self.username_tried_accounts = {}  # New dictionary to track tried accounts per username
        self.new_user_ids = {}  # Add this line to store newly scraped user IDs

    def load_existing_user_ids(self):
        existing_user_ids = {}
        connection = None
        cursor = None
        try:
            connection = self.db_pool.get_connection()
            cursor = connection.cursor(dictionary=True)
            query = """
            SELECT username, user_id FROM user_ids
            WHERE csv_filename = %s
            """
            cursor.execute(query, (self.csv_filename,))
            for row in cursor.fetchall():
                existing_user_ids[row['username']] = row['user_id']
            logger.info(f"Loaded {len(existing_user_ids)} existing user IDs from database.")
        except Error as e:
            logger.error(f"Error loading existing user IDs from database: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return existing_user_ids

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
                # If no accounts are available, find the soonest one to become available
                if self.account_timeouts:
                    soonest_available = min(self.account_timeouts.values())
                    wait_time = max(0, (soonest_available - current_time).total_seconds())
                    if wait_time > 0:
                        logger.info(f"All accounts on timeout. Waiting {wait_time:.2f} seconds for next available account.")
                        self.display_account_status()
                        time.sleep(wait_time)
                    # After waiting, check again for available accounts
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

    def fetch_user_id(self, username, account):
        account = self.check_and_update_cookie(account)
        
        url = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"
        
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

        cookies = {}
        for cookie in account['cookies'].split(';'):
            if '=' in cookie:
                name, value = cookie.strip().split('=', 1)
                cookies[name] = value

        proxies = {
            'http': f"http://{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}",
            'https': f"http://{account['proxy_username']}:{account['proxy_password']}@{account['proxy_address']}:{account['proxy_port']}"
        }

        try:
            response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
            self.last_response_text = response.text  # Store the response text
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'user' in data['data'] and 'id' in data['data']['user']:
                    user_id = data['data']['user']['id']
                    return user_id
                else:
                    logger.warning(f"User data not found for {username}")
                    return "NOT_FOUND"
            elif response.status_code == 404:
                logger.info(f"Username {username} not found (404 error)")
                return "NOT_FOUND"
            else:
                logger.warning(f"Failed to fetch user ID for {username}. Status code: {response.status_code}")
                logger.info(f"Response: {self.last_response_text}")
                return None
        except Exception as e:
            logger.error(f"Error occurred while fetching user ID for {username}: {str(e)}.")
            self.last_response_text = str(e)  # Store the error message
            if "argument of type 'NoneType' is not iterable" in str(e):
                logger.info(f"Marking {username} as NOT_FOUND due to NoneType error")
                return "NOT_FOUND"
            return None

    def scrape_user_ids(self):
        start_time = time.time()
        total_usernames = len(self.usernames)
        processed_count = 0

        with ThreadPoolExecutor(max_workers=len(self.account_data)) as executor:
            futures = []
            for username in self.usernames:
                if username not in self.processed_usernames and username not in self.existing_user_ids:
                    futures.append(executor.submit(self.process_single_username, username))
                elif username in self.existing_user_ids:
                    logger.info(f"Skipping {username} as it already exists in the database.")
                    self.processed_usernames.add(username)
                    processed_count += 1
            
            total_to_process = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                future.result()
                processed_count += 1
                
                if i % 2 == 0 or i == total_to_process:
                    self.display_account_status()
                    self.display_progress(processed_count, total_usernames, start_time)

        self.save_results()
        logger.info(f"Scraping completed. Processed {len(self.processed_usernames)} usernames.")
        return self.new_user_ids  # Add this line to return the newly scraped user IDs

    def display_progress(self, processed_count, total_usernames, start_time):
        elapsed_time = time.time() - start_time
        progress_percentage = (processed_count / total_usernames) * 100
        
        logger.info(f"Progress: {processed_count}/{total_usernames} ({progress_percentage:.2f}%)")

        if elapsed_time > 0:
            overall_rate = self.successful_fetches / elapsed_time
            logger.info(f"Overall processing rate: {overall_rate:.2f} successful usernames/second")
            logger.info(f"Total successful fetches: {self.successful_fetches}")
            
            if overall_rate > 0:
                remaining_usernames = total_usernames - self.successful_fetches
                estimated_time_remaining = remaining_usernames / overall_rate
                logger.info(f"Estimated time remaining: {timedelta(seconds=int(estimated_time_remaining))}")
            else:
                logger.info("Estimated time remaining: Unable to calculate (processing rate is 0)")
        else:
            logger.info("Estimated time remaining: Calculating...")

        # Display additional information about unsuccessful attempts
        unsuccessful_attempts = processed_count - self.successful_fetches
        if unsuccessful_attempts > 0:
            unsuccessful_rate = unsuccessful_attempts / elapsed_time
            logger.info(f"Unsuccessful attempts: {unsuccessful_attempts}")
            logger.info(f"Unsuccessful rate: {unsuccessful_rate:.2f} usernames/second")

    def wait_with_jitter(self, account_id):
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
        jitter = max(jitter, 15)

        logger.info(f"Waiting for {jitter:.2f} seconds for account {account_id}.")
        self.account_jitter_info[account_id] = (jitter, time.time())
        time.sleep(jitter)

    def process_single_username(self, username):
        logger.info(f"Processing username {username}")
        max_retries = 5
        retries = 0
        self.username_tried_accounts[username] = set()  # Initialize set of tried accounts for this username

        while retries < max_retries:
            account = None
            while account is None:
                account = self.get_next_available_account()
                if account is None:
                    logger.warning(f"No available accounts. Waiting before retry for username {username}")
                    self.display_account_status()
                    time.sleep(5)
                elif account['id'] in self.username_tried_accounts[username]:
                    logger.info(f"Account {account['id']} already tried for {username}, looking for another account")
                    account = None

            if account:
                self.username_tried_accounts[username].add(account['id'])

            try:
                self.display_account_status()
                user_id = self.fetch_user_id(username, account)
                if user_id == "NOT_FOUND":
                    logger.info(f"Username {username} not found. Skipping.")
                    self.processed_usernames.add(username)
                    self.wait_with_jitter(account['id'])
                    return
                elif user_id:
                    self.save_user_id(username, user_id)
                    self.processed_usernames.add(username)
                    self.new_user_ids[username] = user_id  # Add this line
                    with self.account_lock:
                        self.successful_fetches += 1  # Increment the counter for successful fetches
                    logger.info(f"Successfully processed username {username}")
                    self.wait_with_jitter(account['id'])
                    return
                else:
                    logger.warning(f"Failed to fetch user ID for username {username}")
                    self.set_account_timeout(account['id'])
            except Exception as e:
                logger.error(f"Error occurred while scraping username {username}: {str(e)}")
                logger.error(traceback.format_exc())
                self.set_account_timeout(account['id'])

            self.wait_with_jitter(account['id'])
            retries += 1

        logger.error(f"Max retries reached for username {username}. Skipping.")

    def set_account_timeout(self, account_id):
        timeout_until = datetime.now() + timedelta(minutes=5)
        with self.account_lock:
            self.account_timeouts[account_id] = timeout_until
        logger.info(f"Account {account_id} set on timeout until {timeout_until}")
        self.display_account_status()  # Display account status after setting a timeout

    def save_user_id(self, username, user_id):
        connection = None
        cursor = None
        try:
            connection = self.db_pool.get_connection()
            cursor = connection.cursor()

            query = """
            INSERT INTO user_ids (username, user_id, csv_filename)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)
            """
            cursor.execute(query, (username, user_id, self.csv_filename))
            connection.commit()

        except Error as e:
            logger.error(f"Error saving user ID to database: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def save_results(self):
        try:
            connection = mysql.connector.connect(**self.db_config)
            cursor = connection.cursor()

            query = """
            INSERT INTO user_ids (username, user_id, csv_filename)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE user_id = VALUES(user_id)
            """
            
            for username, user_id in self.existing_user_ids.items():
                cursor.execute(query, (username, user_id, self.csv_filename))
            
            connection.commit()
            logger.info(f"Saved {len(self.existing_user_ids)} user IDs to database.")
        except Error as e:
            logger.error(f"Error saving results to database: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

        # Optionally, you can still write to CSV if needed
        with open(self.csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Input', 'Username', 'User ID'])
            for username, user_id in self.existing_user_ids.items():
                writer.writerow([username, username, user_id])
        logger.info(f"Saved results to CSV file: {self.csv_filename}")

def main():
    import sys
    if len(sys.argv) != 5:
        print("Usage: python v4_userid_scraper.py <usernames_json> <csv_filename> <account_data_json> <db_config_json>")
        sys.exit(1)

    usernames = json.loads(sys.argv[1])
    csv_filename = sys.argv[2]
    account_data = sys.argv[3]
    db_config = sys.argv[4]

    scraper = InstagramUserIDScraper(usernames, csv_filename, account_data, db_config)
    logger.info(f"Scraping user IDs for {len(usernames)} usernames")
    logger.info(f"Using accounts with IDs: {', '.join(str(id) for id in scraper.account_id_to_index.keys())}")
    scraper.display_account_status()  # Display initial account status
    scraper.scrape_user_ids()

if __name__ == "__main__":
    main()
