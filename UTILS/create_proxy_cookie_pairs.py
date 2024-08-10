import csv
import random
import json

def load_accounts(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def load_cookies(file_path):
    with open(file_path, 'r') as f:
        cookies = []
        for line in f:
            line = line.strip()
            if line:
                # Remove surrounding quotes and replace escaped quotes
                line = line.strip("'").replace('\\"', '"')
                # Split the line into name and cookie
                parts = line.split(' - ', 1)
                if len(parts) == 2:
                    name, cookie = parts
                else:
                    name = f"Unnamed {len(cookies) + 1}"
                    cookie = line
                cookies.append({'name': name, 'cookie': cookie})
        return cookies

def create_pairs(accounts, cookies):
    # Ensure we have the same number of accounts and cookies
    min_length = min(len(accounts), len(cookies))
    accounts = accounts[:min_length]
    cookies = cookies[:min_length]

    # Shuffle both lists
    random.shuffle(accounts)
    random.shuffle(cookies)

    # Create pairs
    pairs = [{'proxy': account['proxy'], 'cookie': cookie['cookie'], 'name': cookie['name'], 'user_agent': account['user_agent']} for account, cookie in zip(accounts, cookies)]
    return pairs

def save_pairs_to_csv(pairs, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Proxy', 'Cookie', 'Name', 'User Agent'])
        for pair in pairs:
            writer.writerow([pair['proxy'], pair['cookie'], pair['name'], pair['user_agent']])

def save_pairs_to_json(pairs, output_file):
    with open(output_file, 'w') as f:
        json.dump(pairs, f, indent=2)

def main():
    accounts = load_accounts('Files/instagram_accounts.json')
    cookies = load_cookies('Files/cookies.txt')

    pairs = create_pairs(accounts, cookies)

    # Save pairs to JSON
    save_pairs_to_json(pairs, 'Files/proxy_cookie_pairs.json')
    save_pairs_to_csv(pairs, 'Files/proxy_cookie_pairs.csv')

    print(f"Created {len(pairs)} proxy-cookie pairs.")
    print("Pairs saved to 'proxy_cookie_pairs.csv' and 'proxy_cookie_pairs.json'.")

if __name__ == "__main__":
    main()