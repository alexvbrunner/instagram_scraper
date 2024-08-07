"""
Instagram Follower Scraper

This script scrapes followers for multiple Instagram user IDs loaded from a CSV file. It uses a list of proxy and cookie pairs
to make requests to Instagram's API, avoiding rate limiting and IP blocks.

The script fetches followers in batches, saves them to a MySQL database table for each user, and can resume from where it left off
using a saved 'max_id' for each user.

Features:
- Processes multiple user IDs from a CSV file
- Uses proxy rotation to avoid IP blocks
- Implements wait times with jitter to mimic human behavior
- Saves progress regularly and can resume from last known position for each user
- Handles request exceptions and continues scraping

Usage:
    Ensure the following files are present:
    - 'Files/proxy_cookie_pairs.json': JSON file containing proxy and cookie pairs
    - 'Files/user_ids.csv': CSV file with user IDs to scrape
    - 'Files/maxid/last_max_id_{user_id}.txt': (Optional) Text files with the last max_id for resuming scraping for each user

    Run the script to populate the 'followers' table in the MySQL database with the scraped follower data for each user.

Note: This script requires pandas, requests, mysql-connector-python, and custom utility functions to be installed.
"""
import config
import time
import requests
import pandas as pd
import random
import json
import csv
from UTILS.utils import wait_with_jitter
import mysql.connector
from mysql.connector import Error
import gender_guesser.detector as gender

def load_proxy_cookie_pairs(file_path):
    with open(file_path, 'r') as f:
        pairs = json.load(f)
    return [(pair['proxy'], pair['cookie']) for pair in pairs]

def load_user_ids(file_path):
    user_ids = []
    with open(file_path, 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            user_ids.append(row['User ID'])
    return list(set(user_ids))  # Remove duplicates

def create_db_connection():
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
        return None
    
def guess_gender(name):
    d = gender.Detector()
    name_parts = name.split()
    if name_parts:
        first_name = name_parts[0]
        return d.get_gender(first_name)
    return 'unknown'  # Return 'unknown' if name is empty or has no parts

def insert_followers(connection, source_account, followers):
    cursor = connection.cursor()
    query = """
    INSERT INTO followers (source_account, pk, pk_id, username, full_name, is_private, fbid_v2, 
    third_party_downloads_enabled, strong_id, profile_pic_id, profile_pic_url, is_verified, 
    has_anonymous_profile_picture, account_badges, latest_reel_media, is_favorite, gender)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    full_name = VALUES(full_name),
    is_private = VALUES(is_private),
    profile_pic_url = VALUES(profile_pic_url),
    is_verified = VALUES(is_verified),
    gender = VALUES(gender)
    """
    for follower in followers:
        gender = guess_gender(follower.get('full_name', ''))
        data = (
            source_account,
            follower.get('pk'),
            follower.get('pk_id'),
            follower.get('username'),
            follower.get('full_name'),
            follower.get('is_private'),
            follower.get('fbid_v2'),
            follower.get('third_party_downloads_enabled'),
            follower.get('strong_id__'),
            follower.get('profile_pic_id'),
            follower.get('profile_pic_url'),
            follower.get('is_verified'),
            follower.get('has_anonymous_profile_picture'),
            json.dumps(follower.get('account_badges')),
            follower.get('latest_reel_media'),
            follower.get('is_favorite'),
            gender  # Add this line
        )
        cursor.execute(query, data)
    connection.commit()
    cursor.close()

def get_followers(user_id, proxy_cookie_pairs, db_connection):
    base_url = f"https://i.instagram.com/api/v1/friendships/{user_id}/followers/"
    params = {'count': 100}
    followers = []
    next_max_id = None

    # Load the last max_id if exists to resume from there
    try:
        with open(f'Files/maxid/last_max_id_{user_id}.txt', 'r') as file:
            next_max_id = file.read().strip()
            if next_max_id:
                params['max_id'] = next_max_id
    except FileNotFoundError:
        pass

    while True:

        # Randomly select a (proxy, cookie) pair
        proxy, cookie = random.choice(proxy_cookie_pairs)
        print(f"Using proxy: {proxy}")

        wait_with_jitter()

        try:
            # Parse proxy string
            proxy_parts = proxy.split(':')
            proxy_url = f"http://{proxy_parts[2]}:{proxy_parts[3]}@{proxy_parts[0]}:{proxy_parts[1]}"

            response = requests.get(base_url, params=params, headers={
                'User-Agent': 'Instagram 275.0.0.27.98 Android (33/13; 420dpi; 1080x2400; samsung; SM-G991B; o1s; exynos2100; en_US; 458229258)',
                'Accept-Language': 'en-US',
                'Accept-Encoding': 'gzip, deflate',
                'X-IG-Capabilities': '3brTvw==',
                'X-IG-Connection-Type': 'WIFI',
                'X-IG-App-ID': '567067343352427',
                'Cookie': cookie
            }, proxies={'http': proxy_url, 'https': proxy_url}, timeout=10)
            response.raise_for_status()
   
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            continue

        data = response.json()

        if 'next_max_id' in data:
            params['max_id'] = data['next_max_id']
            # Save the next_max_id to resume later if needed
            with open(f'Files/maxid/last_max_id_{user_id}.txt', 'w') as file:
                file.write(params['max_id'])
        elif 'next_max_id' not in data:
            print("No next_max_id found, exiting loop.")
            break

        if not data.get('users'):
            print("No more users found, exiting loop.")
            break

        followers.extend(data['users'])

        # Insert followers into the database every 25 followers
        if len(followers) >= 25:
            insert_followers(db_connection, user_id, followers)
            followers = []  # Reset the list after inserting

    # Insert any remaining followers
    if followers:
        insert_followers(db_connection, user_id, followers)
        print(f"Saved remaining followers for user {user_id}.")

# Main execution
if __name__ == "__main__":
    proxy_cookie_pairs = load_proxy_cookie_pairs('Files/proxy_cookie_pairs.json')
    user_ids = load_user_ids('Files/user_ids.csv')
    
    db_connection = create_db_connection()
    if not db_connection:
        print("Failed to connect to the database. Exiting.")
        exit(1)

    for user_id in user_ids:
        print(f"Processing user ID: {user_id}")
        get_followers(user_id, proxy_cookie_pairs, db_connection)
        print(f"Finished processing user ID: {user_id}")
        print("="*50)

    db_connection.close()