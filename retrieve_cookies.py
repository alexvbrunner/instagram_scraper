import requests
import time
import json

def login_to_instagram(username, password):
    session = requests.Session()
    
    # Instagram login URL
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    
    # Headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://www.instagram.com/accounts/login/',
        'x-csrftoken': 'missing'
    }
    
    # Get the CSRF token
    session.get('https://www.instagram.com/accounts/login/')
    csrf_token = session.cookies.get_dict().get('csrftoken')
    
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
    login_response = session.post(login_url, data=login_data, headers=headers)

    print(login_response.json())
    
    # Check if login was successful
    if login_response.json().get('authenticated'):
        print("Login successful!")
        cookies = session.cookies.get_dict()
        cookie_string = '; '.join([f"{k}={v}" for k, v in cookies.items()])
        return cookie_string
    else:
        print("Login failed.")
        return None

def retrieve_cookies_from_json():
    try:
        with open('Files/instagram_accounts.json', 'r') as f:
            accounts = json.load(f)
        
        for account in accounts:
            username = account['username']
            password = account['password']
            print(f"\nAttempting to retrieve cookie for {username}")
            
            cookie = login_to_instagram(username, password)
            
            if cookie:
                print(f"Cookie retrieved successfully for {username}")
                with open('Files/cookies.txt', 'a') as f:
                    f.write(f"\n{username} - '{cookie}'")
                print(f"Cookie for {username} saved to 'Files/cookies.txt'")
            else:
                print(f"Failed to retrieve cookie for {username}")
        
        print("\nAll accounts processed.")
    except FileNotFoundError:
        print("Error: 'Files/instagram_accounts.json' not found.")
    except json.JSONDecodeError:
        print("Error: Invalid JSON in 'Files/instagram_accounts.json'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

def main():
    print("1. Enter credentials manually")
    print("2. Retrieve cookies from JSON file")
    choice = input("Enter your choice (1 or 2): ")
    
    if choice == '1':
        username = input("Enter your Instagram username: ")
        password = input("Enter your Instagram password: ")
        
        cookie = login_to_instagram(username, password)
        
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