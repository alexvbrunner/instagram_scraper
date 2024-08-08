import json
import os

JSON_FILE = 'Files/instagram_accounts.json'

def load_accounts():
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, 'r') as f:
            return json.load(f)
    return []

def save_accounts(accounts):
    with open(JSON_FILE, 'w') as f:
        json.dump(accounts, f, indent=2)

def add_account():
    username = input("Enter Instagram username: ")
    password = input("Enter Instagram password: ")
    
    accounts = load_accounts()
    accounts.append({"username": username, "password": password})
    save_accounts(accounts)
    print(f"Account {username} added successfully.")

def view_accounts():
    accounts = load_accounts()
    if not accounts:
        print("No accounts found.")
    else:
        for i, account in enumerate(accounts, 1):
            print(f"{i}. Username: {account['username']}")

def main():
    while True:
        print("\n1. Add Instagram account")
        print("2. View accounts")
        print("3. Exit")
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            add_account()
        elif choice == '2':
            view_accounts()
        elif choice == '3':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()