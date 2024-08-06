import csv
import random
import json

def load_proxies(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def load_cookies(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def create_pairs(proxies, cookies):
    # Ensure we have the same number of proxies and cookies
    min_length = min(len(proxies), len(cookies))
    proxies = proxies[:min_length]
    cookies = cookies[:min_length]

    # Shuffle both lists
    random.shuffle(proxies)
    random.shuffle(cookies)

    # Create pairs
    return list(zip(proxies, cookies))

def save_pairs_to_csv(pairs, output_file):
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Proxy', 'Cookie'])
        writer.writerows(pairs)

def save_pairs_to_json(pairs, output_file):
    pairs_dict = [{'proxy': proxy, 'cookie': cookie} for proxy, cookie in pairs]
    with open(output_file, 'w') as f:
        json.dump(pairs_dict, f, indent=2)

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