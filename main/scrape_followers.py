import sys
import csv
import json
import random
from datetime import datetime
import time
import traceback
import logging
import os

# Add the Scrapers directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Scrapers.v4_scraper import main as v4_scraper_main
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

def get_user_ids_from_csv(csv_filename):
    user_ids = []
    csv_filename = f'Files/{csv_filename}.csv'
    with open(csv_filename, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            if 'User ID' in row and row['User ID']:
                user_ids.append(row['User ID'])
    return user_ids

def load_user_state(user_id):
    try:
        with open(f'Files/States/{user_id}_state.json', 'r') as f:
            state = json.load(f)
        return state
    except FileNotFoundError:
        return None

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

    csv_filename = input("Enter the CSV filename containing user IDs to scrape without the .csv extension: ")
    user_ids = get_user_ids_from_csv(csv_filename)

    if not user_ids:
        logger.error(f"No valid user IDs found in {csv_filename}.csv. Please check the file format.")
        sys.exit(1)

    logger.info(f"Found {len(user_ids)} user IDs to scrape.")
    logger.info(f"Number of accounts selected for scraping: {num_accounts}")

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
            
            scraped_counts[user_id] = scraped_count
            total_scraped += scraped_count
            logger.info(f"User ID: {user_id} - Followers already scraped: {scraped_count}")
            if scraping_status in ['completed', 'stopped', 'error']:
                logger.info(f"Skipping User ID: {user_id} - Status: {scraping_status}, Reason: {scraping_stop_reason}")
                skipped_user_ids.append(user_id)
                continue
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

    account_data = prepare_account_data(selected_accounts)

    for user_id in user_ids:
        initial_count = scraped_counts.get(user_id, 0)
        logger.info(f"Starting scrape for User ID: {user_id} - Initial follower count: {initial_count}")
        
        # Call v4_scraper main function directly
        try:
            account_ids = [account['id'] for account in account_data]
            logger.info(f"Using accounts with IDs: {', '.join(map(str, account_ids))}")
            
            account_data_json = json.dumps(account_data)
            db_config_json = json.dumps(db_config)
            
            v4_scraper_args = [
                str(user_id),
                csv_filename,
                account_data_json,
                db_config_json
            ]
                    
            original_argv = sys.argv
            sys.argv = ['v4_scraper.py'] + v4_scraper_args
            
            v4_scraper_main()
            
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

    # Update last_checked for used accounts
    for account in account_data:
        update_account_last_checked(connection, account['id'])

    connection.close()

    logger.info("Scraping process completed for all user IDs.")
    logger.info("Final scraped follower counts per user ID:")
    logger.info(json.dumps(scraped_counts, indent=2))
    logger.info(f"Final total scraped followers from all user IDs: {total_scraped}")
    logger.info(f"Skipped user IDs: {skipped_user_ids}")

if __name__ == "__main__":
    main()
