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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramUserIDScraper:
    def __init__(self, usernames, csv_filename, account_data, db_config):
        self.usernames = usernames
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.db_pool = mysql.connector.connect(**self.db_config)
        self.account_queue = []
        self.account_id_to_index = {}
        self.setup_accounts()
        self.processed_usernames = set()
        self.existing_user_ids = self.load_existing_user_ids(csv_filename)
        self.account_timeouts = {}  # New attribute to track account timeouts

    def load_existing_user_ids(self, csv_filename):
        existing_user_ids = {}
        try:
            with open(csv_filename, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    existing_user_ids[row['Username']] = row['User ID']
        except FileNotFoundError:
            logger.info(f"Existing CSV file {csv_filename} not found. Starting fresh.")
        return existing_user_ids

    def setup_accounts(self):
        for i, account in enumerate(self.account_data):
            self.account_queue.append(account)
            self.account_id_to_index[account['id']] = i

    def get_next_available_account(self):
        current_time = datetime.now()
        available_accounts = [account for account in self.account_queue 
                              if account['id'] not in self.account_timeouts or 
                              current_time > self.account_timeouts[account['id']]]
        
        if not available_accounts:
            return None
        
        account = available_accounts.pop(0)
        self.account_queue.remove(account)
        return account

    def display_account_status(self):
        current_time = datetime.now()
        active_accounts = [account['id'] for account in self.account_queue 
                           if account['id'] not in self.account_timeouts or 
                           current_time > self.account_timeouts[account['id']]]
        cooldown_accounts = [account_id for account_id, timeout in self.account_timeouts.items() 
                             if current_time <= timeout]
        
        logger.info("Account Status:")
        logger.info(f"Active accounts: {', '.join(map(str, active_accounts))}")
        logger.info(f"Accounts in cooldown: {', '.join(map(str, cooldown_accounts))}")

    def get_new_cookie_from_db(self, account_id, old_cookie):
        if not self.db_pool.is_connected():
            logger.error("Database connection is not available. Attempting to reconnect...")
            try:
                self.db_pool.reconnect(attempts=3, delay=5)
            except Error as e:
                logger.error(f"Failed to reconnect to database: {e}")
                return None

        cursor = None
        try:
            cursor = self.db_pool.cursor(dictionary=True)
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
                logger.info(f"Response: {response.text}")
                self.last_response_text = response.text
                return None
        except Exception as e:
            logger.error(f"Error occurred while fetching user ID for {username}: {str(e)}")
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
                    logger.info(f"Skipping {username} as it already exists in the CSV file.")
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

    def display_progress(self, processed_count, total_usernames, start_time):
        elapsed_time = time.time() - start_time
        progress_percentage = (processed_count / total_usernames) * 100
        
        if processed_count > 0:
            average_time_per_username = elapsed_time / processed_count
            estimated_total_time = average_time_per_username * total_usernames
            estimated_time_remaining = estimated_total_time - elapsed_time
            
            logger.info(f"Progress: {processed_count}/{total_usernames} ({progress_percentage:.2f}%)")
            logger.info(f"Estimated time remaining: {timedelta(seconds=int(estimated_time_remaining))}")
        else:
            logger.info(f"Progress: {processed_count}/{total_usernames} ({progress_percentage:.2f}%)")
            logger.info("Estimated time remaining: Calculating...")

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
        jitter = max(jitter, 15)

        logger.info(f"Waiting for {jitter:.2f} seconds.")
        time.sleep(jitter)

    def process_single_username(self, username):
        logger.info(f"Processing username {username}")
        while True:
            account = self.get_next_available_account()
            if account is None:
                logger.warning(f"No available accounts. Waiting before retry for username {username}")
                time.sleep(5)
                continue

            try:
                self.display_account_status()
                user_id = self.fetch_user_id(username, account)
                if user_id == "NOT_FOUND":
                    logger.info(f"Username {username} not found. Skipping.")
                    self.processed_usernames.add(username)
                    return
                elif user_id:
                    self.save_user_id(username, user_id)
                    self.processed_usernames.add(username)
                    logger.info(f"Successfully processed username {username}")
                    self.wait_with_jitter()
                    self.account_queue.append(account)
                    return
                else:
                    logger.warning(f"Failed to fetch user ID for username {username}")
                    response_text = self.last_response_text  # Assume this is stored during fetch_user_id
                    if "User not found" in response_text:
                        logger.info(f"Username {username} not found. Skipping.")
                        return
                    self.set_account_timeout(account['id'])
            except Exception as e:
                logger.error(f"Error occurred while scraping username {username}: {str(e)}")
                logger.error(traceback.format_exc())
                self.set_account_timeout(account['id'])

            self.wait_with_jitter()  # Add jittered wait between retries

    def set_account_timeout(self, account_id):
        timeout_until = datetime.now() + timedelta(minutes=5)
        self.account_timeouts[account_id] = timeout_until
        logger.info(f"Account {account_id} set on timeout until {timeout_until}")

    def save_user_id(self, username, user_id):
        try:
            connection = mysql.connector.connect(**self.db_config)
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
            if connection.is_connected():
                cursor.close()
                connection.close()

    def save_results(self):
        with open(self.csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Input', 'Username', 'User ID'])
            
            # Write existing user IDs
            for username, user_id in self.existing_user_ids.items():
                writer.writerow([username, username, user_id])

            try:
                connection = mysql.connector.connect(**self.db_config)
                cursor = connection.cursor()

                query = """
                SELECT username, user_id FROM user_ids
                WHERE csv_filename = %s AND username NOT IN %s
                """
                existing_usernames = tuple(self.existing_user_ids.keys()) or ('',)
                cursor.execute(query, (self.csv_filename, existing_usernames))
                
                for row in cursor.fetchall():
                    writer.writerow([row[0], row[0], row[1]])

            except Error as e:
                logger.error(f"Error fetching results from database: {e}")
            finally:
                if connection.is_connected():
                    cursor.close()
                    connection.close()

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