import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db_config = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password',
    'database': 'instagram_accounts'
}

def get_database_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL database: {e}")
        raise

def get_accounts_from_database(connection, time_threshold=1):
    try:
        cursor = connection.cursor(dictionary=True)
        
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(days=time_threshold)
        
        cursor.execute("""
            SELECT id, proxy_url, cookies, user_agent, last_checked, is_valid
            FROM accounts 
            WHERE is_valid = TRUE 
              AND cookies IS NOT NULL
              AND last_checked > %s
              AND is_valid = 1
        """, (cutoff_time,))
        
        accounts = cursor.fetchall()
        cursor.close()
        
        valid_accounts = []
        for account in accounts:
            last_checked = account['last_checked']
            age = current_time - last_checked
            if age <= timedelta(days=time_threshold):
                valid_accounts.append(account)
                logger.info(f"Account ID: {account['id']}, Last Checked: {age} ago")
            else:
                logger.info(f"Skipping Account ID: {account['id']}, Last Checked: {age} ago (too old)")
        
        logger.info(f"Total accounts: {len(accounts)}, Valid accounts: {len(valid_accounts)}")
        
        return valid_accounts
    except Error as e:
        logger.error(f"Error fetching accounts from database: {e}")
        raise

def parse_proxy_url(proxy_url):
    if not proxy_url:
        return None, None, None, None
    
    parts = proxy_url.split(':')
    if len(parts) == 2:
        return parts[0], parts[1], None, None
    elif len(parts) == 4:
        return parts[0], parts[1], parts[2], parts[3]
    else:
        logger.warning(f"Invalid proxy URL format: {proxy_url}")
        return None, None, None, None

def prepare_account_data(accounts):
    account_data = []
    for account in accounts:
        account_data.append({
            'id': account['id'],
            'proxy_url': account['proxy_url'],
            'cookies': account['cookies'],
            'user_agent': account['user_agent']
        })
    return account_data

def update_account_last_checked(connection, account_id):
    try:
        cursor = connection.cursor()
        current_time = datetime.now()
        
        cursor.execute("""
            UPDATE accounts
            SET last_checked = %s
            WHERE id = %s
        """, (current_time, account_id))
        
        connection.commit()
        cursor.close()
        logger.info(f"Updated last_checked for Account ID: {account_id}")
    except Error as e:
        logger.error(f"Error updating last_checked for Account ID {account_id}: {e}")
        raise

def mark_account_invalid(connection, account_id):
    try:
        cursor = connection.cursor()
        
        cursor.execute("""
            UPDATE accounts
            SET is_valid = FALSE
            WHERE id = %s
        """, (account_id,))
        
        connection.commit()
        cursor.close()
        logger.info(f"Marked Account ID: {account_id} as invalid")
    except Error as e:
        logger.error(f"Error marking Account ID {account_id} as invalid: {e}")
        raise