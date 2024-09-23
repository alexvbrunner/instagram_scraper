import requests
import csv
import sys
import os
import json
import random
from datetime import datetime, timedelta
import logging
import mysql.connector
from mysql.connector import Error
import time

# Add the Scrapers directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Scrapers.v4_userid_scraper import InstagramUserIDScraper
from Scrapers.tagged_scraper import InstagramTaggedScraper

# ClickUp API configuration
CLICKUP_API_KEY = "pk_36436044_38D41W302D9XIE59WEGJJ8PT87Q8FE9X"
FOLDER_ID = "90121508608"
CLICKUP_API_URL = f"https://api.clickup.com/api/v2/folder/{FOLDER_ID}/list"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'database': 'main'
}

def get_database_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        sys.exit(1)

def get_accounts_from_database(connection):
    try:
        cursor = connection.cursor(dictionary=True)
        
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(minutes=15)
        
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
        
        valid_accounts = []
        for account in accounts:
            cookie_time = account['cookie_timestamp']
            if isinstance(cookie_time, str):
                cookie_time = datetime.fromisoformat(cookie_time.replace('T', ' ').split('.')[0])
            age = current_time - cookie_time
            if age <= timedelta(minutes=15):
                valid_accounts.append(account)
                logger.info(f"Account ID: {account['id']}, Cookie Age: {age}")
            else:
                logger.info(f"Skipping Account ID: {account['id']}, Cookie Age: {age} (too old)")
        
        logger.info(f"Total accounts: {len(accounts)}, Valid accounts: {len(valid_accounts)}")
        
        return valid_accounts
    except Error as e:
        logger.error(f"Error fetching accounts from database: {e}")
        sys.exit(1)

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
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # Prepare the query
        placeholders = ', '.join(['%s'] * len(usernames))
        query = f"""
        SELECT username, user_id 
        FROM user_ids 
        WHERE username IN ({placeholders})
        """

        # Execute the query
        cursor.execute(query, tuple(usernames))

        # Fetch all results
        results = cursor.fetchall()

        # Process results
        for row in results:
            user_ids[row['username']] = row['user_id']

        logger.info(f"Retrieved {len(user_ids)} user IDs from database")

        # Identify usernames without user IDs
        missing_usernames = set(usernames) - set(user_ids.keys())
        if missing_usernames:
            logger.warning(f"Missing user IDs for {len(missing_usernames)} usernames. Scraping them now.")
            
            # Scrape missing user IDs
            csv_filename = f"user_ids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            scraper = InstagramUserIDScraper(list(missing_usernames), csv_filename, json.dumps(account_data), json.dumps(db_config))
            new_user_ids = scraper.scrape_user_ids()
            
            # Update user_ids with newly scraped IDs
            user_ids.update(new_user_ids)
            
            logger.info(f"Scraped {len(new_user_ids)} new user IDs")

    except Error as e:
        logger.error(f"Error querying user IDs from database: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

    return user_ids

def get_target_user_id(username, account_data, db_config):
    existing_user_ids = get_user_ids([username], account_data, db_config)
    if username in existing_user_ids:
        logger.info(f"Found existing user ID for target username {username}")
        return existing_user_ids[username]

    csv_filename = f"target_user_id_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        scraper = InstagramUserIDScraper([username], csv_filename, json.dumps(account_data), json.dumps(db_config))
        new_user_ids = scraper.scrape_user_ids()
        user_id = new_user_ids.get(username)
        
        if user_id:
            logger.info(f"Successfully retrieved user ID for target username {username}")
            existing_user_ids[username] = user_id
            return user_id
        
        retry_count += 1
        logger.warning(f"Failed to get user ID for target username {username}. Retry {retry_count}/{max_retries}")
        time.sleep(5)  # Wait 5 seconds before retrying

    logger.error(f"Failed to get user ID for target username {username} after {max_retries} attempts")
    return None

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

    account_ids = [account['id'] for account in account_data]
    logger.info(f"Using accounts with IDs: {', '.join(map(str, account_ids))}")

    # Get list ID based on list name
    list_name = 'Ecobelleza'
    target_username = 'ecobelleza.oficial'
    try:
        list_id = get_list_id(list_name)
        logger.info(f"Found list ID: {list_id}")
    except ValueError as e:
        logger.error(str(e))
        return

    # Fetch data from ClickUp
    try:
        tasks = fetch_clickup_data(list_id)
        logger.info(f"Fetched {len(tasks)} tasks from ClickUp")
        usernames = extract_usernames(tasks)
        logger.info(f"Extracted {len(usernames)} usernames with non-null Instagram handles")
    except Exception as e:
        logger.error(f"Error fetching data from ClickUp: {str(e)}")
        return

    if not usernames:
        logger.info("No tasks found with Instagram handles and 'check posting' status.")
        return

    logger.info(f"Found {len(usernames)} usernames to process.")

    # Get user IDs (from database and scrape if necessary)
    user_ids = get_user_ids(usernames, account_data, db_config)

    # Get target user ID
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

        # Update ClickUp status for successful taggers
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