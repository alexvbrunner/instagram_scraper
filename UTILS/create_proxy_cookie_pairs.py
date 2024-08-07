import csv
import random
import json

def load_proxies(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_cookies(file_path):
    with open(file_path, 'r') as f:
        cookies = []
        for line in f:
            line = line.strip()
            if line:
                # Remove surrounding quotes and replace escaped quotes
                line = line.strip("'").replace('\\"', '"')
                cookies.append(line)
        return cookies

def create_pairs(proxies, cookies):
    # Ensure we have the same number of proxies and cookies
    min_length = min(len(proxies), len(cookies))
    proxies = proxies[:min_length]
    cookies = cookies[:min_length]

    # Shuffle both lists
    random.shuffle(proxies)
    random.shuffle(cookies)

    # Create pairs with optional random names
    pairs = [{'proxy': proxy, 'cookie': cookie, 'name': f"Pair {i + 1}"} for i, (proxy, cookie) in enumerate(zip(proxies, cookies))]
    return pairs

def save_pairs_to_csv(pairs, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Proxy', 'Cookie', 'Name'])
        for pair in pairs:
            writer.writerow([pair['proxy'], pair['cookie'], pair['name']])

def save_pairs_to_json(pairs, output_file):
    with open(output_file, 'w') as f:
        json.dump(pairs, f, indent=2)

def main():
    proxies = load_proxies('Files/Webshare 10 proxies.txt')
    cookies = load_cookies('Files/cookies.txt')

    pairs = create_pairs(proxies, cookies)

    # Save pairs to CSV
    save_pairs_to_csv(pairs, 'Files/proxy_cookie_pairs.csv')

    # Save pairs to JSON
    save_pairs_to_json(pairs, 'Files/proxy_cookie_pairs.json')

    print(f"Created {len(pairs)} proxy-cookie pairs.")
    print("Pairs saved to 'proxy_cookie_pairs.csv' and 'proxy_cookie_pairs.json'.")

if __name__ == "__main__":
    main()