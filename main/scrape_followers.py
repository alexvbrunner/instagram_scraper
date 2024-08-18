import mysql.connector
import subprocess
import sys
from mysql.connector import Error
import csv
import json
import random
from datetime import datetime, timedelta
import time
import queue

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
        choice = input("Do you want to use proxies? (yes/no): ").lower()
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

def main():
    connection = get_database_connection()
    accounts = get_accounts_from_database(connection)
    connection.close()

    if not accounts:
        print("No accounts found in the database with Instagram created and valid cookies.")
        sys.exit(1)

    total_accounts = len(accounts)
    print(f"Total available accounts: {total_accounts}")

    while True:
        try:
            num_accounts = int(input(f"Enter the number of accounts to use for scraping (1-{total_accounts}): "))
            if 1 <= num_accounts <= total_accounts:
                break
            else:
                print(f"Please enter a number between 1 and {total_accounts}.")
        except ValueError:
            print("Please enter a valid number.")

    # Randomly select the specified number of accounts
    selected_accounts = random.sample(accounts, num_accounts)

    use_proxies = get_proxy_preference()

    csv_filename = input("Enter the CSV filename containing user IDs to scrape without the .csv extension: ")
    user_ids = get_user_ids_from_csv(csv_filename)

    if not user_ids:
        print(f"No valid user IDs found in {csv_filename}.csv. Please check the file format.")
        sys.exit(1)

    print(f"Found {len(user_ids)} user IDs to scrape.")

    # Randomize the order of user IDs
    random.shuffle(user_ids)

    for user_id in user_ids:
        print(f"Scraping followers for User ID: {user_id}")
        
        # Prepare account data for v3_scraper.py
        account_data = []
        for account in selected_accounts:
            account_data.append({
                'proxy_address': account['proxy_address'],
                'proxy_port': account['proxy_port'],
                'proxy_username': account['proxy_username'],
                'proxy_password': account['proxy_password'],
                'cookies': account['cookies'],
                'user_agent': account['user_agent']
            })
        
        # Run v3_scraper.py with the current user ID and selected account information
        try:
            subprocess.run([
                sys.executable, 
                "Scrapers/v3_scraper.py", 
                str(user_id),
                csv_filename,
                use_proxies,
                json.dumps(account_data)
            ], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error occurred while scraping User ID {user_id}: {e}")
            continue

        print(f"Finished scraping for User ID: {user_id}")
        print("-" * 50)

    print("Scraping process completed for all user IDs.")

if __name__ == "__main__":
    main()