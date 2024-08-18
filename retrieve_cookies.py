import requests
import time
import json
import random
import codecs
from selenium.common.exceptions import WebDriverException
import pyotp
import re
import argparse

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"

import sys
import select

def is_escape_pressed():
    rlist, _, _ = select.select([sys.stdin], [], [], 0.1)
    if rlist:
        key = sys.stdin.read(1)
        return key == '\x1b'
    return False

def login_to_instagram(username, password, user_agent, totp_secret=None):
    session = requests.Session()
    
    # Instagram login URL
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    
    # Headers to mimic a browser
    headers = {
        'User-Agent': user_agent,
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.instagram.com/accounts/login/',
        'x-csrftoken': 'missing',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Origin': 'https://www.instagram.com',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    
    # Get the CSRF token
    try:
        response = session.get('https://www.instagram.com/accounts/login/', headers=headers)
        csrf_token = response.cookies.get('csrftoken', 'missing')
    except requests.exceptions.RequestException as e:
        print(f"Error getting CSRF token: {e}")
        return None
    
    # Prepare login data
    login_data = {
        'username': username,
        'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{int(time.time())}:{password}',
        'queryParams': {},
        'optIntoOneTap': 'false'
    }
    
    # Update headers with CSRF token
    headers['x-csrftoken'] = csrf_token
    
    # Attempt login
    try:
        login_response = session.post(login_url, data=login_data, headers=headers)
        print(f"Response status code: {login_response.status_code}")
        print(f"Response headers: {login_response.headers}")
        print(f"Response content: {login_response.text}")

        if login_response.status_code == 200 or login_response.status_code == 400:
            try:
                response_json = login_response.json()
                if response_json.get('authenticated'):
                    print("Login successful!")
                    
                    # Perform additional actions to enrich the cookie
                    print("Performing additional actions...")
                    
                    # Visit the user's profile
                    session.get(f'https://www.instagram.com/{username}/', headers=headers)
                    time.sleep(random.uniform(2, 4))
                    
                    # Visit the explore page
                    session.get('https://www.instagram.com/explore/', headers=headers)
                    time.sleep(random.uniform(2, 4))
                    
                    # Visit and interact with multiple popular hashtags
                    popular_hashtags = ['love', 'instagood', 'fashion', 'photooftheday', 'art', 'photography', 'travel', 'food', 'fitness', 'nature']
                    for _ in range(3):
                        random_hashtag = random.choice(popular_hashtags)
                        session.get(f'https://www.instagram.com/explore/tags/{random_hashtag}/', headers=headers)
                        time.sleep(random.uniform(3, 5))
                        
                        # Simulate scrolling
                        for _ in range(random.randint(3, 6)):
                            session.get(f'https://www.instagram.com/explore/tags/{random_hashtag}/?__a=1&__d=dis', headers=headers)
                            time.sleep(random.uniform(1, 2))
                    
                    # Visit a few random user profiles
                    for _ in range(2):
                        random_user = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=random.randint(5, 10)))
                        session.get(f'https://www.instagram.com/{random_user}/', headers=headers)
                        time.sleep(random.uniform(2, 4))
                    
                    # Perform a search query
                    search_query = random.choice(['cats', 'dogs', 'travel', 'food', 'fashion'])
                    session.get(f'https://www.instagram.com/web/search/topsearch/?query={search_query}', headers=headers)
                    time.sleep(random.uniform(2, 4))
                    
                    # Visit Instagram Direct
                    session.get('https://www.instagram.com/direct/inbox/', headers=headers)
                    time.sleep(random.uniform(2, 4))
                    
                    # Now retrieve the enriched cookies
                    cookies = session.cookies.get_dict()
                    cookie_string = '; '.join([f"{k}={v}" for k, v in cookies.items()])
                    return cookie_string
                elif response_json.get('two_factor_required'):
                    print("Two-factor authentication required.")
                    two_factor_info = response_json['two_factor_info']
                    two_factor_identifier = two_factor_info['two_factor_identifier']
                    
                    if totp_secret:
                        # Remove spaces and convert to uppercase
                        totp_secret = re.sub(r'\s+', '', totp_secret).upper()
                        try:
                            totp = pyotp.TOTP(totp_secret)
                            verification_code = totp.now()
                            print(f"Generated 2FA code: {verification_code}")
                        except Exception as e:
                            print(f"Error generating 2FA code: {str(e)}")
                            print("Please enter the 6-digit code from your authenticator app:")
                            verification_code = input().strip()
                    else:
                        print(f"Two-factor authentication required for {username}")
                        print("Please enter the 6-digit code from your authenticator app:")
                        verification_code = input().strip()
                    
                    # Submit 2FA code
                    two_factor_url = 'https://www.instagram.com/accounts/login/ajax/two_factor/'
                    two_factor_data = {
                        'username': username,
                        'verificationCode': verification_code,
                        'identifier': two_factor_identifier,
                        'queryParams': {}
                    }
                    two_factor_response = session.post(two_factor_url, data=two_factor_data, headers=headers)
                    two_factor_json = two_factor_response.json()
                    
                    if two_factor_json.get('authenticated'):
                        print("Two-factor authentication successful!")
                        # Perform additional actions to enrich the cookie
                        print("Performing additional actions...")
                        
                        # Visit the user's profile
                        session.get(f'https://www.instagram.com/{username}/', headers=headers)
                        time.sleep(random.uniform(1, 3))
                        
                        # Visit the explore page
                        session.get('https://www.instagram.com/explore/', headers=headers)
                        time.sleep(random.uniform(1, 3))
                        
                        # Visit a random popular hashtag
                        popular_hashtags = ['love', 'instagood', 'fashion', 'photooftheday', 'art', 'photography']
                        random_hashtag = random.choice(popular_hashtags)
                        session.get(f'https://www.instagram.com/explore/tags/{random_hashtag}/', headers=headers)
                        time.sleep(random.uniform(1, 3))
                        
                        # Now retrieve the enriched cookies
                        cookies = session.cookies.get_dict()
                        cookie_string = '; '.join([f"{k}={v}" for k, v in cookies.items()])
                        return cookie_string
                    else:
                        print("Two-factor authentication failed.")
                        return None
                elif 'checkpoint_required' in response_json or 'checkpoint_url' in response_json:
                    print("Checkpoint required. Please log in manually using these credentials:")
                    print(f"Username: {username}")
                    print(f"Password: {password}")
                    print("Press Enter after completing the login and checkpoint process, or press Escape to skip this account.")
                    
                    while True:
                        if is_escape_pressed():
                            print("Account skipped.")
                            return None
                        if sys.stdin in select.select([sys.stdin], [], [], 0.1)[0]:
                            line = sys.stdin.readline()
                            if line.strip() == '':
                                break
                    
                    # After manual login and checkpoint completion, perform additional actions
                    print("Performing additional actions after manual login...")
                    
                    # Visit the user's profile
                    session.get(f'https://www.instagram.com/{username}/', headers=headers)
                    time.sleep(random.uniform(1, 3))
                    
                    # Visit the explore page
                    session.get('https://www.instagram.com/explore/', headers=headers)
                    time.sleep(random.uniform(1, 3))
                    
                    # Visit a random popular hashtag
                    popular_hashtags = ['love', 'instagood', 'fashion', 'photooftheday', 'art', 'photography']
                    random_hashtag = random.choice(popular_hashtags)
                    session.get(f'https://www.instagram.com/explore/tags/{random_hashtag}/', headers=headers)
                    time.sleep(random.uniform(1, 3))
                    
                    # Now retrieve the enriched cookies
                    cookies = session.cookies.get_dict()
                    cookie_string = '; '.join([f"{k}={v}" for k, v in cookies.items()])
                    
                    if cookie_string:
                        print("Enriched cookies retrieved after manual login.")
                        return cookie_string
                    else:
                        print("Failed to retrieve cookies after manual login.")
                        return None
                else:
                    print("Login failed.")
                    return None
            except json.JSONDecodeError:
                print("Failed to decode JSON response.")
                return None
        else:
            print(f"Login failed with status code: {login_response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error during login request: {e}")
        return None

def retrieve_cookies_from_json():
    try:
        # Try different encodings
        encodings = ['utf-8', 'utf-8-sig', 'utf-16', 'ascii']
        content = None
        
        for encoding in encodings:
            try:
                with codecs.open('Files/instagram_accounts.json', 'r', encoding=encoding) as f:
                    content = f.read().strip()
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            raise ValueError("Unable to read the file with any of the attempted encodings.")
        
        # Remove BOM if present
        if content.startswith(u'\ufeff'):
            content = content[1:]
        
        # Remove any leading/trailing whitespace and non-printable characters
        content = ''.join(char for char in content if char.isprintable() or char.isspace())
        
        # Parse JSON
        accounts = json.loads(content)
        
        for account in accounts:
            try:
                username = account['username']
                password = account['password']
                user_agent = account.get('user_agent', USER_AGENT)
                totp_secret = account.get('totp_secret', None)
                print(f"\nAttempting to retrieve cookie for {username}")
                print(f"Using user agent: {user_agent}")
                
                max_retries = 3
                for attempt in range(max_retries):
                    cookie = login_to_instagram(username, password, user_agent, totp_secret)
                    if cookie is None:
                        print(f"Skipping account {username}")
                        break
                    elif cookie:
                        print(f"Cookie retrieved successfully for {username}")
                        with open('Files/cookies.txt', 'a', encoding='utf-8') as f:
                            f.write(f"\n{username} - '{cookie}'")
                        print(f"Cookie for {username} saved to 'Files/cookies.txt'")
                        break
                    else:
                        print(f"Failed to retrieve cookie for {username} (Attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            delay = (2 ** attempt) + random.random()
                            print(f"Retrying in {delay:.2f} seconds...")
                            time.sleep(delay)
                else:
                    print(f"Failed to retrieve cookie for {username} after {max_retries} attempts")
            except Exception as e:
                print(f"Error processing account {username}: {str(e)}")
                continue
            
            # Add a delay between accounts to avoid rate limiting
            time.sleep(random.uniform(5, 10))
        
        print("\nAll accounts processed.")
    except FileNotFoundError:
        print("Error: 'Files/instagram_accounts.json' not found.")
    except json.JSONDecodeError as j:
        print(f"Error: Invalid JSON in 'Files/instagram_accounts.json'. {j}")
        print("Content of the file:")
        print(content)
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    print("1. Enter credentials manually")
    print("2. Retrieve cookies from JSON file")
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == '1':
        username = input("Enter your Instagram username: ")
        password = input("Enter your Instagram password: ")
        user_agent = input("Enter your user agent (leave blank to use a default mobile user agent): ")
        
        if not user_agent:
            user_agent = random.choice(USER_AGENT)
        
        cookie = login_to_instagram(username, password, user_agent)
        
        if cookie:
            print("\nCookie retrieved successfully:")
            print(cookie)
            
            with open('Files/cookies.txt', 'a') as f:
                f.write(f"\n{username} - '{cookie}'")
            print("\nCookie saved to 'Files/cookies.txt'")
        else:
            print("Failed to retrieve cookie.")
    elif choice == '2':
        retrieve_cookies_from_json()
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main()