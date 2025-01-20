from consensus_economics.aws.bucket_manager import BucketManager
from consensus_economics.paths import Paths
from datetime import datetime
import argparse

import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

def clean_bucket(bucket):
    # Get all contents
    contents = bucket.contents
    
    # Iterate through all files in the bucket
    for item in contents:
        key = item['Key']
        # Check if key contains " 2"
        # Check if key contains any " N" where N is an integer
        for i in range(1, 10):  # Checking numbers 1-9
            if f" {i}" in key:
                # Create new key by removing " N" 
                new_key = key.replace(f" {i}", "")
                
                # If the target file already exists, remove it
                if any(x['Key'] == new_key for x in contents):
                    bucket.remove_file(new_key)
                
                # Rename the file by removing and reuploading with new key
                content = bucket.get_content(key)
                metadata = bucket.get_metadata(key)
                bucket.upload_file(content.read(), new_key, metadata)
                bucket.remove_file(key)
                break  # Break after first match since we've renamed the file

def get_s3_key(file_path, processed_dir):
    return os.path.relpath(file_path, processed_dir).replace('processed/', '').replace('//', '/')

def set_metadata(file_path, processed_dir):
    # Get just the filename without the full path
    filename = os.path.basename(file_path)
    return {
        'source': 'consensus_economics',
        'year': filename[:4],
        'month': filename[4:6],
        'date_uploaded': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'file_type': os.path.splitext(file_path)[1][1:]
    }

def read_file(file_path):
    with open(file_path, 'rb') as f:
        return f.read()

def upload_file(args):
    bucket, file_path, processed_dir = args
    s3_key = get_s3_key(file_path, processed_dir)
    file_content = read_file(file_path)
    bucket.upload_file(
        file_content=file_content,
        file_path=s3_key,
        metadata=set_metadata(file_path, processed_dir)
    )
    return s3_key

def get_files_for_year(processed_dir: str, year: int) -> list:
    """Get all files for a specific year."""
    files_to_upload = []
    year_dir = os.path.join(processed_dir, str(year))
    
    if not os.path.exists(year_dir):
        print(f"No data found for year {year}")
        return files_to_upload
    
    for root, _, files in os.walk(year_dir):
        for file in files:
            file_path = os.path.join(root, file)
            files_to_upload.append(file_path)
    
    return files_to_upload

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Upload Consensus Economics data to S3 bucket')
    parser.add_argument('--year', type=int, help='Year to process (e.g., 2024)')
    parsed_args = parser.parse_args()
    
    if not parsed_args.year:
        print("Please specify a year using --year parameter")
        return
    
    # Initialize bucket
    bucket = BucketManager("consensus-economics")
    paths = Paths()
    
    clean_bucket(bucket)
    
    # Get files for the specified year
    processed_dir = paths.processed
    files_to_upload = get_files_for_year(processed_dir, parsed_args.year)
    
    if not files_to_upload:
        return
    
    print(f"Found {len(files_to_upload)} files to upload for year {parsed_args.year}")
    
    # Prepare arguments for upload
    upload_args = [(bucket, file_path, processed_dir) for file_path in files_to_upload]
    
    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for args in upload_args:
            future = executor.submit(upload_file, args)
            futures.append(future)
        
        # Show progress with tqdm
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc=f"Uploading files for {parsed_args.year}"
        ):
            try:
                s3_key = future.result()
            except Exception as e:
                print(f"Error uploading file: {e}")

if __name__ == "__main__":
    main()
