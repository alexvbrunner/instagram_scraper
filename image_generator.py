import os
import requests
import concurrent.futures
from pathlib import Path

def download_avatar(gender, index):
    url = f"https://xsgames.co/randomusers/avatar.php?g={gender}"
    response = requests.get(url)
    if response.status_code == 200:
        folder_path = Path("Avatars") / "generated_avatars_female"
        folder_path.mkdir(parents=True, exist_ok=True)
        file_path = folder_path / f"{gender}_{index}.png"
        with open(file_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded {gender} avatar {index}")
    else:
        print(f"Failed to download {gender} avatar {index}")

def generate_avatars(num_male, num_female, max_threads=10):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        male_futures = [executor.submit(download_avatar, "male", i) for i in range(num_male)]
        female_futures = [executor.submit(download_avatar, "female", i) for i in range(num_female)]
        
        concurrent.futures.wait(male_futures + female_futures)

if __name__ == "__main__":
    num_male_avatars = 0
    num_female_avatars = 5000
    max_threads = 20

    generate_avatars(num_male_avatars, num_female_avatars, max_threads)
    print("Avatar generation complete!")