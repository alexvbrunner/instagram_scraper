import sys
import csv
import json
import random
from datetime import datetime
import logging
import os

# Add the Scrapers directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Scrapers.v4_data_scraper import main as v4_data_scraper_main
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

def get_user_ids_from_database(connection, csv_filename, table):
    try:
        print(f"Getting user IDs from database for {csv_filename} in {table} table")
        cursor = connection.cursor()
        if table == 'followers':
            query = """
            SELECT DISTINCT pk
            FROM followers
            WHERE csv_filename = %s
            """
        elif table == 'user_ids':
            query = """
            SELECT user_id
            FROM user_ids
            WHERE csv_filename = %s
            """
        else:
            raise ValueError("Invalid table name")
        
        cursor.execute(query, (csv_filename,))
        user_ids = [str(row[0]) for row in cursor.fetchall()]
        cursor.close()
        return user_ids
    except Exception as e:
        logger.error(f"Error fetching user IDs from database: {e}")
        return []

def get_user_ids_from_csv(csv_filename):
    try:
        with open(csv_filename, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            user_ids = [row[0] for row in csv_reader if row]  # Assuming user IDs are in the first column
        return user_ids
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_filename}")
        return []
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return []

def initialize_database():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()

        # Create users table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            username VARCHAR(255) UNIQUE,
            full_name VARCHAR(255),
            biography TEXT,
            follower_count INT,
            following_count INT,
            media_count INT,
            is_private BOOLEAN,
            is_verified BOOLEAN,
            category VARCHAR(255),
            external_url VARCHAR(255),
            public_email VARCHAR(255),
            public_phone_number VARCHAR(255),
            is_business BOOLEAN,
            profile_pic_url TEXT,
            hd_profile_pic_url TEXT,
            has_highlight_reels BOOLEAN,
            has_guides BOOLEAN,
            is_interest_account BOOLEAN,
            total_igtv_videos INT,
            total_clips_count INT,
            total_ar_effects INT,
            is_eligible_for_smb_support_flow BOOLEAN,
            is_eligible_for_lead_center BOOLEAN,
            account_type VARCHAR(255),
            is_call_to_action_enabled BOOLEAN,
            interop_messaging_user_fbid BIGINT,
            has_videos BOOLEAN,
            total_video_count INT,
            has_music_on_profile BOOLEAN,
            is_potential_business BOOLEAN,
            is_memorialized BOOLEAN,
            gender VARCHAR(50),
            csv_filename VARCHAR(255)
        )
        """)

        # Add csv_filename column if it doesn't exist
        cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'users'
        AND COLUMN_NAME = 'csv_filename'
        """)
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
            ALTER TABLE users
            ADD COLUMN csv_filename VARCHAR(255)
            """)
            logger.info("Added csv_filename column to users table")

        # Create bio_links table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bio_links (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT,
            url TEXT,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
        """)

        # Create pinned_channels table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pinned_channels (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT,
            title VARCHAR(255),
            subtitle TEXT,
            invite_link TEXT,
            number_of_members INT,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
        """)

        connection.commit()
        logger.info("Database tables initialized successfully")
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

    while True:
        source = input("Enter 'db' to fetch user IDs from the database or 'csv' to read from a CSV file: ").lower()
        if source in ['db', 'csv']:
            break
        else:
            logger.error("Please enter either 'db' or 'csv'.")

    if source == 'db':
        while True:
            table = input("Enter 'followers' or 'user_ids' to specify which table to fetch user IDs from: ").lower()
            if table in ['followers', 'user_ids']:
                break
            else:
                logger.error("Please enter either 'followers' or 'user_ids'.")
        
        csv_filename = input("Enter the CSV filename to fetch user IDs: ")
        user_ids = get_user_ids_from_database(connection, csv_filename, table)
    else:
        csv_filename = input("Enter the path to the CSV file containing user IDs: ")
        user_ids = get_user_ids_from_csv(f'Files/{csv_filename}.csv')

    connection.close()

    if not user_ids:
        logger.error(f"No valid user IDs found. Please check the {'database' if source == 'db' else 'CSV file'}.")
        sys.exit(1)

    logger.info(f"Found {len(user_ids)} user IDs to scrape.")
    logger.info(f"Number of accounts selected for scraping: {num_accounts}")

    # Initialize database tables
    initialize_database()

    account_data = prepare_account_data(selected_accounts)

    account_ids = [account['id'] for account in account_data]
    logger.info(f"Using accounts with IDs: {', '.join(map(str, account_ids))}")

    account_data_json = json.dumps(account_data)
    db_config_json = json.dumps(db_config)

    v4_data_scraper_args = [
        json.dumps(user_ids),
        csv_filename,
        account_data_json,
        db_config_json
    ]

    original_argv = sys.argv
    sys.argv = ['v4_data_scraper.py'] + v4_data_scraper_args

    v4_data_scraper_main()

    sys.argv = original_argv

    # Update last_checked for used accounts
    connection = get_database_connection()
    for account in account_data:
        update_account_last_checked(connection, account['id'])
    connection.close()

    logger.info("User data scraping process completed for all user IDs.")

if __name__ == "__main__":
    main()
