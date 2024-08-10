import json
import os
import random

JSON_FILE = 'Files/instagram_accounts.json'
PROXIES_FILE = 'proxies.txt'

MOBILE_USER_AGENTS = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/122.0.6261.89 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; SM-A536B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36',
    'Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 12; motorola edge 20 pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) FxiOS/122.0 Mobile/15E148 Safari/605.1.15',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.105 Mobile Safari/537.36'
]

def load_accounts():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r') as f:
            return json.load(f)
    return []

def save_accounts(accounts):
    with open(JSON_FILE, 'w') as f:
        json.dump(accounts, f, indent=2)

def load_proxies():
    if os.path.exists(PROXIES_FILE):
        with open(PROXIES_FILE, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    return []

def get_random_user_agent():
    return random.choice(MOBILE_USER_AGENTS)

def add_account():
    username = input("Enter Instagram username: ")
    password = input("Enter Instagram password: ")
    
    accounts = load_accounts()
    proxies = load_proxies()
    
    if not any(acc['username'] == username for acc in accounts):
        proxy = proxies[len(accounts) % len(proxies)] if proxies else None
        user_agent = get_random_user_agent()
        accounts.append({"username": username, "password": password, "proxy": proxy, "user_agent": user_agent})
        save_accounts(accounts)
        print(f"Account {username} added successfully.")
    else:
        print(f"Account {username} already exists.")

def add_accounts_from_file(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return
    
    accounts = load_accounts()
    proxies = load_proxies()
    existing_usernames = {acc['username'] for acc in accounts}
    
    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) == 2:
                username, password = parts
                if username not in existing_usernames:
                    proxy = proxies[len(accounts) % len(proxies)] if proxies else None
                    user_agent = get_random_user_agent()
                    accounts.append({"username": username, "password": password, "proxy": proxy, "user_agent": user_agent})
                    existing_usernames.add(username)
    
    save_accounts(accounts)
    print(f"Accounts from {file_path} added successfully.")

def view_accounts():
    accounts = load_accounts()
    if not accounts:
        print("No accounts found.")
    else:
        for i, account in enumerate(accounts, 1):
            print(f"{i}. Username: {account['username']}, Proxy: {account.get('proxy', 'None')}, User Agent: {account.get('user_agent', 'None')}")

def main():
    while True:
        print("\n1. Add Instagram account")
        print("2. View accounts")
        print("3. Add accounts from file")
        print("4. Exit")
        choice = input("Enter your choice (1-4): ")
        
        if choice == '1':
            add_account()
        elif choice == '2':
            view_accounts()
        elif choice == '3':
            file_path = input("Enter the path to the accounts file: ")
            add_accounts_from_file(file_path)
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()