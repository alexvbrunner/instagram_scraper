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
        self.failed_usernames = set()
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
            cursor.close()
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

        response = requests.get(url, headers=headers, cookies=cookies, proxies=proxies)
        
        if response.status_code == 200:
            data = response.json()
            user_id = data['data']['user']['id']
            return user_id
        else:
            logger.warning(f"Failed to fetch user ID for {username}. Status code: {response.status_code}")
            logger.info(f"Response: {response.text}")
            return None

    def scrape_user_ids(self):
        with ThreadPoolExecutor(max_workers=len(self.account_data)) as executor:
            futures = []
            for username in self.usernames:
                if username not in self.processed_usernames and username not in self.existing_user_ids:
                    futures.append(executor.submit(self.process_single_username, username))
                elif username in self.existing_user_ids:
                    logger.info(f"Skipping {username} as it already exists in the CSV file.")
                    self.processed_usernames.add(username)
            
            total_usernames = len(futures)
            for i, future in enumerate(as_completed(futures), 1):
                future.result()
                if i % 10 == 0 or i == total_usernames:  # Display status every 10 usernames or at the end
                    self.display_account_status()

        self.save_results()
        logger.info(f"Scraping completed. Processed {len(self.processed_usernames)} usernames.")
        if self.failed_usernames:
            logger.warning(f"Failed to scrape {len(self.failed_usernames)} usernames.")

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
        retry_count = 0
        max_retries = 3

        while retry_count < max_retries:
            account = self.get_next_available_account()
            if account is None:
                logger.warning(f"No available accounts. Waiting before retry for username {username}")
                time.sleep(5)
                retry_count += 1
                continue

            try:
                user_id = self.fetch_user_id(username, account)
                if user_id:
                    self.save_user_id(username, user_id)
                    self.processed_usernames.add(username)
                    logger.info(f"Successfully processed username {username}")
                    self.wait_with_jitter()  # Add jittered wait after successful request
                    self.account_queue.append(account)
                    return
                else:
                    logger.warning(f"Failed to fetch user ID for username {username}")
                    self.set_account_timeout(account['id'])
            except Exception as e:
                logger.error(f"Error occurred while scraping username {username}: {str(e)}")
                logger.error(traceback.format_exc())
                self.set_account_timeout(account['id'])
                retry_count += 1

            self.wait_with_jitter()  # Add jittered wait between retries

        self.failed_usernames.add(username)
        logger.warning(f"Max retries reached for username: {username}")

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