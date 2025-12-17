import requests
import json
import pandas as pd
import time
from datetime import datetime
import os

# Your cookies formatted for requests
cookies = {
    "datr": "HRw4aXCY9aFm4EpHxPXC6vrT",
    "ig_did": "80CC0166-7BF8-4140-A4CB-24732B69ADE6",
    "mid": "aTgcIAALAAGlChW2Eihpn1iRVOI4",
    "csrftoken": "qPLC2WUYY-FlCbqx9_rzyr",
    "ds_user_id": "79641938502",
    "sessionid": "79641938502%3AK3SZUgihNxWpgp%3A15%3AAYjmb2CH8aWeIoAgox_QKxhQ5GxrVrrmkvWlkgxfHw",
    "rur": '"ODN\05479641938502\0541796821338:01fede22e3b79bcceb23ab440dec1a1b174832242a94196c129ae1c91d0996df408e6525"',
    "ig_nrcb": "1",
    "wd": "197x641"
}

headers = {
    "authority": "www.instagram.com",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control": "max-age=0",
    "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "x-csrftoken": cookies["csrftoken"],
    "x-ig-app-id": "936619743392459",
    "x-requested-with": "XMLHttpRequest"
}

def get_profile_data(username):
    """Fetch Instagram profile data for a given username"""
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    
    try:
        response = requests.get(
            url,
            headers=headers,
            cookies=cookies,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Error for {username}: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"Exception for {username}: {e}")
        return None

def extract_user_info(profile_data, username):
    """Extract relevant information from profile data, using 'NA' for missing values"""
    if not profile_data:
        return {
            'username': username,
            'status': 'Failed',
            'full_name': 'NA',
            'followers': 'NA',
            'following': 'NA',
            'posts': 'NA',
            'biography': 'NA',
            'is_verified': 'NA',
            'is_private': 'NA',
            'external_url': 'NA',
            'profile_pic_url': 'NA'
        }
    
    try:
        user = profile_data.get('data', {}).get('user', {})
        
        # Helper function to get value or 'NA'
        def get_value(value):
            if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                return 'NA'
            return value
        
        # Extract nested values with NA fallback
        followers = user.get('edge_followed_by', {}).get('count')
        following = user.get('edge_follow', {}).get('count')
        posts = user.get('edge_owner_to_timeline_media', {}).get('count')
        
        return {
            'username': get_value(user.get('username', username)),
            'status': 'Success',
            'full_name': get_value(user.get('full_name')),
            'followers': get_value(followers),
            'following': get_value(following),
            'posts': get_value(posts),
            'biography': get_value(user.get('biography')),
            'is_verified': get_value(user.get('is_verified')),
            'is_private': get_value(user.get('is_private')),
            'external_url': get_value(user.get('external_url')),
            'profile_pic_url': get_value(user.get('profile_pic_url_hd'))
        }
    except Exception as e:
        print(f"Error extracting data for {username}: {e}")
        return {
            'username': username,
            'status': 'Error',
            'full_name': 'NA',
            'followers': 'NA',
            'following': 'NA',
            'posts': 'NA',
            'biography': 'NA',
            'is_verified': 'NA',
            'is_private': 'NA',
            'external_url': 'NA',
            'profile_pic_url': 'NA'
        }

def process_csv_to_excel(input_csv, output_excel=None, username_column='username', delay=2):
    """
    Read usernames from CSV and save extracted data to Excel
    
    Args:
        input_csv: Path to input CSV file
        output_excel: Path to output Excel file (optional)
        username_column: Name of the column containing usernames
        delay: Delay between requests in seconds (to avoid rate limiting)
    """
    # Read CSV file
    try:
        df_input = pd.read_csv(input_csv)
        print(f"Loaded {len(df_input)} usernames from {input_csv}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    # Check if username column exists
    if username_column not in df_input.columns:
        print(f"Column '{username_column}' not found in CSV.")
        print(f"Available columns: {list(df_input.columns)}")
        return
    
    # Create output filename if not provided
    if output_excel is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_excel = f"instagram_profiles_{timestamp}.xlsx"
    
    # List to store all extracted data
    results = []
    
    # Process each username
    total = len(df_input)
    for idx, row in df_input.iterrows():
        username = str(row[username_column]).strip()
        
        # Handle empty or NA usernames
        if not username or username.lower() in ['na', 'nan', 'none', '']:
            print(f"\n[{idx+1}/{total}] Skipping empty username")
            results.append({
                'username': 'NA',
                'status': 'Skipped',
                'full_name': 'NA',
                'followers': 'NA',
                'following': 'NA',
                'posts': 'NA',
                'biography': 'NA',
                'is_verified': 'NA',
                'is_private': 'NA',
                'external_url': 'NA',
                'profile_pic_url': 'NA'
            })
            continue
        
        print(f"\n[{idx+1}/{total}] Processing: {username}")
        
        # Get profile data
        profile_data = get_profile_data(username)
        
        # Extract information
        user_info = extract_user_info(profile_data, username)
        results.append(user_info)
        
        # Print summary
        if user_info['status'] == 'Success':
            followers_display = user_info['followers'] if user_info['followers'] != 'NA' else 'NA'
            posts_display = user_info['posts'] if user_info['posts'] != 'NA' else 'NA'
            name_display = user_info['full_name'] if user_info['full_name'] != 'NA' else 'No Name'
            print(f"  ✓ {name_display} | Followers: {followers_display} | Posts: {posts_display}")
        else:
            print(f"  ✗ Failed to fetch data - All fields set to NA")
        
        # Save incrementally (after each extraction)
        df_results = pd.DataFrame(results)
        
        # Replace any remaining None or NaN values with 'NA'
        df_results = df_results.fillna('NA')
        
        df_results.to_excel(output_excel, index=False, engine='openpyxl')
        print(f"  → Saved to {output_excel}")
        
        # Delay to avoid rate limiting (except for last item)
        if idx < total - 1:
            time.sleep(delay)
    
    # Final processing
    df_results = pd.DataFrame(results)
    df_results = df_results.fillna('NA')  # Replace any NaN with 'NA'
    
    # Save final version
    df_results.to_excel(output_excel, index=False, engine='openpyxl')
    
    # Final summary
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Total processed: {total}")
    print(f"Successful: {sum(1 for r in results if r['status'] == 'Success')}")
    print(f"Failed: {sum(1 for r in results if r['status'] == 'Failed')}")
    print(f"Skipped: {sum(1 for r in results if r['status'] == 'Skipped')}")
    print(f"Output saved to: {output_excel}")
    print(f"{'='*60}")
    
    return df_results

# Example usage
if __name__ == "__main__":
    # Configuration
    INPUT_CSV = r"C:\Users\Usman Asghar\Desktop\Intern\Instagram Web Scraping\Excel\username.csv"  # Your CSV file with usernames
    OUTPUT_EXCEL = r"C:\Users\Usman Asghar\Desktop\Intern\Instagram Web Scraping\Excel\instagram_data.xlsx"  # Output Excel file
    USERNAME_COLUMN = "username"  # Column name in CSV containing usernames
    DELAY_SECONDS = 3  # Delay between requests (adjust as needed)
    
    # Process the CSV file
    results_df = process_csv_to_excel(
        input_csv=INPUT_CSV,
        output_excel=OUTPUT_EXCEL,
        username_column=USERNAME_COLUMN,
        delay=DELAY_SECONDS
    )