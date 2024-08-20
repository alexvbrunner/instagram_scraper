import mysql.connector
import sys
from mysql.connector import Error
import csv
import json
import random
from datetime import datetime, timedelta
import time
import queue
import traceback
import logging
import os

# Add the Scrapers directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Scrapers.v4_scraper import main as v4_scraper_main

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_database_connection():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            database='main',
            user='root',
            password='password'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        sys.exit(1)

def get_accounts_from_database(connection):
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Get the current time
        current_time = datetime.now()
        
        # Calculate the cutoff time (5 hours ago)
        cutoff_time = current_time - timedelta(hours=5)
        
        cursor.execute("""
            SELECT id, proxy_address, proxy_port, proxy_username, proxy_password, 
                   cookies, user_agent, cookie_timestamp
            FROM accounts 
            WHERE instagram_created = TRUE 
              AND cookies IS NOT NULL
              AND status = 'Active'
              AND cookie_timestamp > %s
        """, (cutoff_time,))
        
        accounts = cursor.fetchall()
        cursor.close()
        
        # Filter accounts and print information
        valid_accounts = []
        for account in accounts:
            cookie_time = datetime.strptime(account['cookie_timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
            age = current_time - cookie_time
            if age <= timedelta(hours=5):
                valid_accounts.append(account)
                print(f"Account ID: {account['id']}, Cookie Age: {age}")
            else:
                print(f"Skipping Account ID: {account['id']}, Cookie Age: {age} (too old)")
        
        print(f"Total accounts: {len(accounts)}, Valid accounts: {len(valid_accounts)}")
        
        return valid_accounts
    except Error as e:
        print(f"Error fetching accounts from database: {e}")
        sys.exit(1)

def get_proxy_preference():
    while True:
        choice = 'y'
        if choice in ['yes', 'y']:
            return 'yes'
        elif choice in ['no', 'n']:
            return 'no'
        else:
            print("Invalid choice. Please enter 'yes' or 'no'.")

def get_user_ids_from_csv(csv_filename):
    user_ids = []
    csv_filename = f'Files/{csv_filename}.csv'
    with open(csv_filename, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            if 'User ID' in row and row['User ID']:
                user_ids.append(row['User ID'])
    return user_ids

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

def load_scraped_followers_count(user_id):
    try:
        with open(f'{user_id}_state.json', 'r') as f:
            state = json.load(f)
        return state.get('total_followers_scraped', 0)
    except FileNotFoundError:
        return 0

def load_user_state(user_id):
    try:
        with open(f'{user_id}_state.json', 'r') as f:
            state = json.load(f)
        return state
    except FileNotFoundError:
        return None

def main():
    connection = get_database_connection()
    accounts = get_accounts_from_database(connection)
    connection.close()

    if not accounts:
        logger.error("No accounts found in the database with Instagram created and valid cookies.")
        sys.exit(1)

    total_accounts = len(accounts)
    logger.info(f"Total available accounts: {total_accounts}")

    while True:
        try:
            num_accounts = int(input(f"Enter the number of accounts to use for scraping (1-{total_accounts}): "))
            if 1 <= num_accounts <= total_accounts:
                break
            else:
                logger.error(f"Please enter a number between 1 and {total_accounts}.")
        except ValueError:
            logger.error("Please enter a valid number.")

    # Randomly select the specified number of accounts
    selected_accounts = random.sample(accounts, num_accounts)

    use_proxies = get_proxy_preference()

    csv_filename = input("Enter the CSV filename containing user IDs to scrape without the .csv extension: ")
    user_ids = get_user_ids_from_csv(csv_filename)

    if not user_ids:
        logger.error(f"No valid user IDs found in {csv_filename}.csv. Please check the file format.")
        sys.exit(1)

    logger.info(f"Found {len(user_ids)} user IDs to scrape.")

    # Log the number of accounts and users per account
    logger.info(f"Number of accounts selected for scraping: {num_accounts}")
    users_per_account = len(user_ids) // num_accounts
    logger.info(f"Number of users per account: {users_per_account}")

    # Randomize the order of user IDs
    random.shuffle(user_ids)

    db_config = {
        'host': '127.0.0.1',
        'user': 'root',
        'password': 'password',
        'database': 'main'
    }

    scraped_counts = {}
    total_scraped = 0
    skipped_user_ids = []

    # Load and print initial scraped counts
    logger.info("Initial scraped follower counts:")
    for user_id in user_ids:
        state = load_user_state(user_id)
        if state:
            scraped_count = state.get('total_followers_scraped', 0)
            scraping_status = state.get('scraping_status', 'in_progress')
            scraping_stop_reason = state.get('scraping_stop_reason', None)
            
            if scraping_status in ['completed', 'stopped', 'error']:
                logger.info(f"Skipping User ID: {user_id} - Status: {scraping_status}, Reason: {scraping_stop_reason}")
                skipped_user_ids.append(user_id)
                continue
            
            scraped_counts[user_id] = scraped_count
            total_scraped += scraped_count
            logger.info(f"User ID: {user_id} - Followers already scraped: {scraped_count}")
        else:
            scraped_counts[user_id] = 0
            logger.info(f"User ID: {user_id} - No previous state found")

    # Remove skipped user IDs from the list to scrape
    user_ids = [uid for uid in user_ids if uid not in skipped_user_ids]

    logger.info("Initial scraped follower counts per user ID:")
    logger.info(json.dumps(scraped_counts, indent=2))
    logger.info(f"Initial total scraped followers from all user IDs: {total_scraped}")
    logger.info(f"Skipped user IDs and reasons:")
    for uid in skipped_user_ids:
        state = load_user_state(uid)
        status = state.get('scraping_status', 'unknown')
        reason = state.get('scraping_stop_reason', 'unknown')
        logger.info(f"  User ID: {uid} - Status: {status}, Reason: {reason}")
    logger.info("-" * 50)

    for user_id in user_ids:
        initial_count = scraped_counts.get(user_id, 0)
        logger.info(f"Starting scrape for User ID: {user_id} - Initial follower count: {initial_count}")
        
        # Prepare account data for v4_scraper.py
        account_data = []
        for account in selected_accounts:
            account_data.append({
                'id': account['id'],
                'proxy_address': account['proxy_address'],
                'proxy_port': account['proxy_port'],
                'proxy_username': account['proxy_username'],
                'proxy_password': account['proxy_password'],
                'cookies': account['cookies'],
                'user_agent': account['user_agent']
            })
        
        # Call v4_scraper main function directly
        try:
            # Print the IDs of the accounts being used
            account_ids = [account['id'] for account in account_data]
            logger.info(f"Using accounts with IDs: {', '.join(map(str, account_ids))}")
            
            account_data_json = json.dumps(account_data)
            db_config_json = json.dumps(db_config)
            
            # Prepare arguments for v4_scraper main function
            v4_scraper_args = [
                str(user_id),
                csv_filename,
                account_data_json,
                db_config_json
            ]
                    
            # Temporarily replace sys.argv with our arguments
            original_argv = sys.argv
            sys.argv = ['v4_scraper.py'] + v4_scraper_args
            
            # Call v4_scraper main function
            v4_scraper_main()
            
            # Restore original sys.argv
            sys.argv = original_argv
            
        except Exception as e:
            logger.error(f"Error occurred while scraping User ID {user_id}: {str(e)}")
            logger.error(traceback.format_exc())
            continue

        # Update scraped count after scraping
        state = load_user_state(user_id)
        if state:
            new_count = state.get('total_followers_scraped', 0)
            scraping_status = state.get('scraping_status', 'in_progress')
            scraping_stop_reason = state.get('scraping_stop_reason', None)
            
            scraped_counts[user_id] = new_count
            total_scraped += (new_count - initial_count)
            logger.info(f"Finished scraping for User ID: {user_id} - New follower count: {new_count}")
            logger.info(f"Followers scraped in this session: {new_count - initial_count}")
            logger.info(f"Scraping status: {scraping_status}, Reason: {scraping_stop_reason}")
        else:
            logger.warning(f"No state file found for User ID: {user_id} after scraping")
        
        logger.info("-" * 50)

    logger.info("Scraping process completed for all user IDs.")
    
    # Print the final dictionary of user IDs and their scraped follower counts
    logger.info("Final scraped follower counts per user ID:")
    logger.info(json.dumps(scraped_counts, indent=2))
    
    # Print the final total sum of scraped users
    logger.info(f"Final total scraped followers from all user IDs: {total_scraped}")
    logger.info(f"Skipped user IDs: {skipped_user_ids}")

if __name__ == "__main__":
    main()