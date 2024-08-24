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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InstagramUserIDScraper:
    def __init__(self, usernames, csv_filename, account_data, db_config):
        self.usernames = usernames
        self.csv_filename = csv_filename
        self.account_data = json.loads(account_data)
        self.db_config = json.loads(db_config)
        self.account_queue = []
        self.account_id_to_index = {}
        self.setup_accounts()
        self.processed_usernames = set()
        self.failed_usernames = set()
        self.existing_user_ids = self.load_existing_user_ids(csv_filename)

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
        if not self.account_queue:
            return None
        return self.account_queue.pop(0)

    def scrape_user_ids(self):
        with ThreadPoolExecutor(max_workers=len(self.account_data)) as executor:
            futures = []
            for username in self.usernames:
                if username not in self.processed_usernames and username not in self.existing_user_ids:
                    futures.append(executor.submit(self.process_single_username, username))
                elif username in self.existing_user_ids:
                    logger.info(f"Skipping {username} as it already exists in the CSV file.")
                    self.processed_usernames.add(username)
            
            for future in as_completed(futures):
                future.result()

        self.save_results()
        logger.info(f"Scraping completed. Processed {len(self.processed_usernames)} usernames.")
        if self.failed_usernames:
            logger.warning(f"Failed to scrape {len(self.failed_usernames)} usernames.")

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
                    return
                else:
                    logger.warning(f"Failed to fetch user ID for username {username}")
            except Exception as e:
                logger.error(f"Error occurred while scraping username {username}: {str(e)}")
                logger.error(traceback.format_exc())
                retry_count += 1
            finally:
                self.account_queue.append(account)

        self.failed_usernames.add(username)
        logger.warning(f"Max retries reached for username: {username}")

    def fetch_user_id(self, username, account):
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
    scraper.scrape_user_ids()

if __name__ == "__main__":
    main()