import requests
import json
import pandas as pd
import time
from datetime import datetime
import os

# Your cookies formatted for requests
cookies = {
    ## add your cookies
}

headers = {
    ## add your headers
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
    INPUT_CSV = r"username directory input"  # Your CSV file with usernames
    OUTPUT_EXCEL = r"output directory"  # Output Excel file
    USERNAME_COLUMN = "username"  # Column name in CSV containing usernames
    DELAY_SECONDS = 3  # Delay between requests (adjust as needed)
    
    # Process the CSV file
    results_df = process_csv_to_excel(
        input_csv=INPUT_CSV,
        output_excel=OUTPUT_EXCEL,
        username_column=USERNAME_COLUMN,
        delay=DELAY_SECONDS
    )