from consensus_economics.aws.bucket_manager import BucketManager
from consensus_economics.paths import Paths
from datetime import datetime

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

def main():
    # Initialize bucket
    bucket = BucketManager("consensus-economics")
    paths = Paths()
    
    clean_bucket(bucket)
    
    # Walk through all files in processed directory
    processed_dir = paths.processed 
    
    # Collect all files first
    files_to_upload = []
    for root, _, files in os.walk(processed_dir):
        for file in files:
            file_path = os.path.join(root, file)
            files_to_upload.append((bucket, file_path, processed_dir))
    
    # Use ThreadPoolExecutor for parallel uploads
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for args in files_to_upload:
            future = executor.submit(upload_file, args)
            futures.append(future)
        
        # Show progress with tqdm
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Uploading files"
        ):
            try:
                s3_key = future.result()
            except Exception as e:
                print(f"Error uploading file: {e}")

if __name__ == "__main__":
    main()
