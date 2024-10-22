import requests
import csv
import sys
import os
import json
import random
from datetime import datetime
import logging
import time
import argparse
from collections import Counter

# Add the Scrapers directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Scrapers.v4_userid_scraper import InstagramUserIDScraper
from Scrapers.tagged_scraper import InstagramTaggedScraper
from db_utils import (
    get_database_connection,
    get_accounts_from_database,
    prepare_account_data,
    update_account_last_checked,
    mark_account_invalid,
    db_config
)

# ClickUp API configuration
CLICKUP_API_KEY = "pk_36436044_38D41W302D9XIE59WEGJJ8PT87Q8FE9X"
FOLDER_ID = "90121508608"
CLICKUP_API_URL = f"https://api.clickup.com/api/v2/folder/{FOLDER_ID}/list"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_list_id(list_name):
    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json"
    }
    response = requests.get(CLICKUP_API_URL, headers=headers)
    response.raise_for_status()
    lists = response.json()["lists"]
    for list_item in lists:
        if list_item["name"] == list_name:
            return list_item["id"]
    raise ValueError(f"List '{list_name}' not found in folder {FOLDER_ID}")

def fetch_clickup_data(list_id):
    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json"
    }
    params = {
        "statuses[]": "check posting",
        "include_closed": "true",
        "custom_fields": json.dumps([{
            "field_id": "0aa99d2a-335a-429a-adde-3cd0a1a78d0a",
            "operator": "IS NOT NULL"
        }])
    }
    url = f"https://api.clickup.com/api/v2/list/{list_id}/task"
    
    all_tasks = []
    page = 0
    
    while True:
        try:
            params["page"] = page
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            tasks = data.get("tasks", [])
            
            if not tasks:
                break
            
            all_tasks.extend(tasks)
            page += 1
            
            if data.get("last_page", False):
                break
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from ClickUp: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response content: {e.response.text}")
            raise

    return all_tasks

def extract_usernames(tasks):
    usernames = []
    for task in tasks:
        custom_fields = task.get('custom_fields', [])
        instagram_handle = next((field.get('value') for field in custom_fields 
                                 if field['id'] == "0aa99d2a-335a-429a-adde-3cd0a1a78d0a" and field.get('value')), None)
        if instagram_handle:
            usernames.append(instagram_handle)
    return usernames

def get_user_ids(usernames, account_data, db_config):
    user_ids = {}
    connection = None
    cursor = None
    try:
        connection = get_database_connection()
        cursor = connection.cursor(dictionary=True)

        placeholders = ', '.join(['%s'] * len(usernames))
        query = f"""
        SELECT username, user_id 
        FROM user_ids 
        WHERE username IN ({placeholders})
        """

        cursor.execute(query, tuple(usernames))
        results = cursor.fetchall()

        for row in results:
            user_ids[row['username']] = row['user_id']

        logger.info(f"Retrieved {len(user_ids)} user IDs from database")

        missing_usernames = set(usernames) - set(user_ids.keys())
        if missing_usernames:
            logger.warning(f"Missing user IDs for {len(missing_usernames)} usernames. Scraping them now.")
            
            csv_filename = f"user_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            scraper = InstagramUserIDScraper(list(missing_usernames), csv_filename, json.dumps(account_data), json.dumps(db_config))
            new_user_ids = scraper.scrape_user_ids()
            
            user_ids.update(new_user_ids)
            
            logger.info(f"Scraped {len(new_user_ids)} new user IDs")

    except Exception as e:
        logger.error(f"Error querying user IDs from database: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

    return user_ids

def update_clickup_status(task_id, new_status):
    headers = {
        "Authorization": CLICKUP_API_KEY,
        "Content-Type": "application/json"
    }
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    data = {
        "status": new_status
    }
    try:
        response = requests.put(url, headers=headers, json=data)
        response.raise_for_status()
        logger.info(f"Updated task {task_id} status to {new_status}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error updating ClickUp task status: {str(e)}")

def read_usernames_from_file(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def save_results_to_csv(usernames, user_ids, successful_taggers, csv_filename):
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Username', 'User ID', 'Number of Tagged Posts']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for username in usernames:
            user_id = user_ids.get(username, 'N/A')
            tagged_posts_count = Counter(successful_taggers)[str(user_id)] if user_id != 'N/A' else 0
            writer.writerow({
                'Username': username,
                'User ID': user_id,
                'Number of Tagged Posts': tagged_posts_count
            })
    
    logger.info(f"Results saved to {csv_filename}")

def main():
    parser = argparse.ArgumentParser(description="Instagram Tagged Post Scraper")
    parser.add_argument("--file", help="Path to text file containing usernames")
    args = parser.parse_args()

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

    account_data = prepare_account_data(selected_accounts)

    account_ids = [account['id'] for account in account_data]
    logger.info(f"Using accounts with IDs: {', '.join(map(str, account_ids))}")

    if args.file:
        usernames = read_usernames_from_file(args.file)
        logger.info(f"Read {len(usernames)} usernames from file: {args.file}")
    else:
        list_name = 'Ecobelleza'
        try:
            list_id = get_list_id(list_name)
            logger.info(f"Found list ID: {list_id}")
            tasks = fetch_clickup_data(list_id)
            logger.info(f"Fetched {len(tasks)} tasks from ClickUp")
            usernames = extract_usernames(tasks)
            logger.info(f"Extracted {len(usernames)} usernames with non-null Instagram handles")
        except Exception as e:
            logger.error(f"Error fetching data from ClickUp: {str(e)}")
            return

    if not usernames:
        logger.info("No usernames found to process.")
        return

    logger.info(f"Found {len(usernames)} usernames to process.")

    user_ids = get_user_ids(usernames, account_data, db_config)

    target_username = 'ecobelleza.oficial'
    target_user_id = get_user_ids([target_username], account_data, db_config).get(target_username)
    
    if not target_user_id:
        logger.error(f"Could not find or scrape user ID for target username: {target_username}. Exiting.")
        return

    logger.info(f"Target user ID for {target_username}: {target_user_id}")

    # Prepare user_data for tagged scraper
    user_data = {str(user_id): username for username, user_id in user_ids.items()}

    # Define csv_filename
    csv_filename = f"tagged_posts/tagged_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Run tagged scraper
    if user_data:
        scraper = InstagramTaggedScraper(user_data, target_user_id, target_username, csv_filename, json.dumps(account_data), json.dumps(db_config))
        logger.info(f"Scraping tagged posts for {len(user_data)} users")
        logger.info(f"Using accounts with IDs: {', '.join(str(id) for id in scraper.account_id_to_index.keys())}")
        scraper.display_account_status()
        scraper.scrape_tagged_posts()
        successful_taggers = scraper.successful_taggers
        logger.info("Tagged post scraping process completed.")

        # Save results to CSV
        results_csv_filename = f"tagged_posts_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        save_results_to_csv(usernames, user_ids, successful_taggers, results_csv_filename)

        # Update ClickUp status only if not using file input
        if not args.file:
            for task in tasks:
                custom_fields = task.get('custom_fields', [])
                instagram_handle = next((field.get('value') for field in custom_fields 
                                         if field['id'] == "0aa99d2a-335a-429a-adde-3cd0a1a78d0a" and field.get('value')), None)
                if instagram_handle:
                    user_id = user_ids.get(instagram_handle)
                    if user_id and str(user_id) in successful_taggers:
                        update_clickup_status(task['id'], "aftercare")
                        logger.info(f"Updated ClickUp status to 'aftercare' for user {instagram_handle} (ID: {user_id})")

    else:
        logger.warning("No user IDs found to scrape tagged posts. Skipping tagged post scraping.")


if __name__ == "__main__":
    main()
