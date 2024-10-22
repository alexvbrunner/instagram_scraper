import sys
import csv
import json
import random
from datetime import datetime
import logging
import os

# Add the Scrapers directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Scrapers.v4_userid_scraper import main as v4_userid_scraper_main
from db_utils import (
    get_database_connection,
    get_accounts_from_database,
    prepare_account_data,
    update_account_last_checked,
    mark_account_invalid,
    db_config
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_usernames_from_file(filename):
    usernames = []
    with open(filename, 'r') as file:
        for line in file:
            username = line.strip().split('|')[-1].strip()
            if username:
                usernames.append(username)
    return usernames

def initialize_database():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_ids (
            username VARCHAR(255) PRIMARY KEY,
            user_id BIGINT,
            csv_filename VARCHAR(255)
        )
        """)

        connection.commit()
        logger.info("Database table initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def main():
    connection = get_database_connection()
    accounts = get_accounts_from_database(connection)

    if not accounts:
        logger.error("No accounts found in the database with Instagram created and valid cookies.")
        connection.close()
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

    selected_accounts = random.sample(accounts, num_accounts)

    input_filename = input("Enter the input filename containing usernames (e.g., input_usernames.txt): ")
    if input_filename == "":
        input_filename = "input_usernames.txt"
    print(f"Input filename: {input_filename}")
    usernames = get_usernames_from_file(input_filename)

    if not usernames:
        logger.error(f"No valid usernames found in the file: {input_filename}. Please check the file.")
        sys.exit(1)

    logger.info(f"Found {len(usernames)} usernames to scrape.")
    logger.info(f"Number of accounts selected for scraping: {num_accounts}")

    # Initialize database table
    initialize_database()

    account_data = prepare_account_data(selected_accounts)

    account_ids = [account['id'] for account in account_data]
    logger.info(f"Using accounts with IDs: {', '.join(map(str, account_ids))}")

    account_data_json = json.dumps(account_data)
    db_config_json = json.dumps(db_config)

    existing_csv_filename = input("Enter the existing CSV filename (e.g., Files/user_ids.csv): ")
    existing_csv_filename = f"{existing_csv_filename}.csv"
    output_filename = f"user_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    v4_userid_scraper_args = [
        json.dumps(usernames),
        existing_csv_filename,
        account_data_json,
        db_config_json
    ]

    original_argv = sys.argv
    sys.argv = ['v4_userid_scraper.py'] + v4_userid_scraper_args

    v4_userid_scraper_main()

    sys.argv = original_argv

    # Update last_checked for used accounts
    for account in account_data:
        update_account_last_checked(connection, account['id'])

    connection.close()

    logger.info(f"User ID scraping process completed. Results saved to {output_filename}")

if __name__ == "__main__":
    main()
