import os
import time
import urllib.request
from urllib.parse import urljoin, unquote
from html.parser import HTMLParser
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Robust Network Helper ---
def fetch_with_retry(req, retries=3, timeout=15):
    """Wraps urllib requests with explicit timeouts and exponential backoff retries."""
    for attempt in range(retries):
        try:
            return urllib.request.urlopen(req, timeout=timeout)
        except Exception as e:
            if attempt == retries - 1:
                raise e  # Propagate the error on the final attempt
            print(f"[~] Network hiccup ({e}). Retrying in {2 ** attempt}s...")
            time.sleep(2 ** attempt)

# --- HTML Parsing Logic ---
class DirectoryLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href':
                    self.links.append(value)

def get_remote_files(base_url, headers, current_rel_dir=""):
    """Recursively scans the web directory."""
    url = base_url + current_rel_dir
    print(f"[*] Scanning remote directory: {url}")
    
    req = urllib.request.Request(url, headers=headers)
    try:
        with fetch_with_retry(req) as response:
            html_content = response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        # If we can't read a directory, we raise to fail the whole process
        raise RuntimeError(f"Failed to access directory {url}: {e}")

    parser = DirectoryLinkParser()
    parser.feed(html_content)
    
    files = []
    for href in parser.links:
        if not href or href in ('../', '..') or href.startswith('?'):
            continue
            
        full_url = urljoin(url, href)
        if not full_url.startswith(url) or full_url == url:
            continue
            
        name = unquote(full_url.rstrip('/').split('/')[-1])
        
        if href.endswith('/'):
            files.extend(get_remote_files(base_url, headers, current_rel_dir + name + '/'))
        else:
            files.append((full_url, current_rel_dir + name))
            
    return files

# --- Worker Thread Logic ---
def process_single_file(file_info, headers, bucket_name, s3_prefix, table_name):
    """Worker function executed by the ThreadPoolExecutor. Raises exceptions on failure."""
    file_url, rel_path = file_info
    s3_key = f"{s3_prefix}{rel_path}".replace('\\', '/')
    
    session = boto3.Session()
    s3 = session.client('s3')
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # 1. Fetch Headers
    head_req = urllib.request.Request(file_url, headers=headers, method='HEAD')
    try:
        with fetch_with_retry(head_req) as response:
            server_modified = response.headers.get('Last-Modified', '')
    except Exception as e:
        raise RuntimeError(f"Failed to fetch headers for {rel_path}: {e}")

    # 2. Check State
    try:
        db_response = table.get_item(Key={'url': file_url})
        db_item = db_response.get('Item')
    except ClientError as e:
        raise RuntimeError(f"DynamoDB read error for {rel_path}: {e}")

    # 3. Stream and Sync
    if not db_item or db_item.get('last_modified') != server_modified:
        get_req = urllib.request.Request(file_url, headers=headers)
        try:
            with fetch_with_retry(get_req, timeout=30) as response: # Longer timeout for actual download
                s3.upload_fileobj(response, bucket_name, s3_key)
            
            table.put_item(Item={
                'url': file_url,
                's3_key': s3_key,
                'last_modified': server_modified
            })
            return f"[+] Success (New/Updated): {rel_path} -> s3://{bucket_name}/{s3_key}"
        except Exception as e:
            raise RuntimeError(f"Upload failed for {rel_path}: {e}")
    else:
        return f"[-] Skipped (Up to date): {rel_path}"

# --- Main Engine ---
def multi_thread_sync(base_url, headers, bucket_name, s3_prefix, table_name, max_threads=10):
    if not base_url.endswith('/'):
        base_url += '/'
    if s3_prefix and not s3_prefix.endswith('/'):
        s3_prefix += '/'

    print("--- Phase 0: Pre-Flight AWS Check ---")
    try:
        boto3.client('dynamodb').describe_table(TableName=table_name)
        print(f"[*] Confirmed access to DynamoDB Table: '{table_name}'")
    except Exception as e:
        # RAISE instead of return
        raise RuntimeError(f"FATAL: Cannot connect to DynamoDB table '{table_name}'. {e}")

    # 1. Pull the state FIRST
    print("\n--- Phase 1: Pulling State from DynamoDB ---")
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    s3 = boto3.client('s3')

    try:
        response = table.scan()
        db_items = response.get('Items', [])
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            db_items.extend(response.get('Items', []))
    except ClientError as e:
        raise RuntimeError(f"Failed to pull state tracking inventory from DynamoDB: {e}")

    # 2. Immediately execute the safeguard if the table is empty
    is_pristine_run = False
    if not db_items:
        print("[-] DynamoDB state table is empty. Purging target S3 path for clean sync...")
        try:
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
                if 'Contents' in page:
                    delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
                    s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_keys})
            print("[*] S3 target path cleared successfully.")
            is_pristine_run = True
        except Exception as e:
            raise RuntimeError(f"Error while cleaning target S3 path: {e}")
    else:
        print(f"[*] Loaded {len(db_items)} state records from DynamoDB.")

    # 3. Safe to scan the host server now
    print("\n--- Phase 2: Scanning Host Server ---")
    remote_files = get_remote_files(base_url, headers)
    remote_urls = set([f[0] for f in remote_files])

    if not remote_files:
        raise RuntimeError("No remote files discovered on the target server. Aborting.")

    # 4. Stream to S3
    print(f"\n--- Phase 3: Multi-Threaded Sync ({max_threads} workers) ---")
    failed_workers = 0
    
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_url = {
            executor.submit(
                process_single_file, file_info, headers, bucket_name, s3_prefix, table_name
            ): file_info for file_info in remote_files
        }
        
        for future in as_completed(future_to_url):
            try:
                # If process_single_file raised an error, it gets re-raised here
                result_msg = future.result()
                print(result_msg)
            except Exception as exc:
                print(f"[!] Worker Error: {exc}")
                failed_workers += 1

    # 5. Clean orphans using the initial Phase 1 snapshot
    if not is_pristine_run:
        print("\n--- Phase 4: Orphan Cleanup ---")
        for item in db_items:
            db_url = item['url']
            db_s3_key = item['s3_key']
            
            if db_url not in remote_urls:
                print(f"[x] Removing deleted file from S3: s3://{bucket_name}/{db_s3_key}")
                try:
                    s3.delete_object(Bucket=bucket_name, Key=db_s3_key)
                    table.delete_item(Key={'url': db_url})
                except Exception as e:
                    print(f"[!] Cleanup reconciliation failure for key {db_s3_key}: {e}")
                    failed_workers += 1
    else:
        print("\n--- Phase 4: Orphan Cleanup (Skipped - Pristine Run) ---")

    # Final health check
    if failed_workers > 0:
        raise RuntimeError(f"Sync cycle finished, but encountered {failed_workers} fatal errors. See logs.")

    print("\n[*] Multi-Threaded Cloud Sync Engine Cycle Complete.")

def handler(event, context):
    """
    AWS Lambda Entry Point
    """
    print("[-] Starting BLS Data Sync Lambda...")
    
    TARGET_URL = "https://download.bls.gov/pub/time.series/pr/" 
    HEADERS = {
        'User-Agent': 'hello@gmail.com',
        'Sec-Ch-Ua-Platform': '"Linux"'
    }

    S3_BUCKET_NAME = os.environ['BUCKET_NAME']
    S3_STORE_PREFIX = "data/bls_data"          
    WORKER_THREADS = 2 
    DYNAMODB_TABLE_NAME = os.environ.get('TABLE_NAME')

    # Trigger the main engine. We don't need a try/except here because Lambda 
    # natively handles raised exceptions and reports them to Step Functions correctly.
    multi_thread_sync(
        base_url=TARGET_URL,
        headers=HEADERS,
        bucket_name=S3_BUCKET_NAME,
        s3_prefix=S3_STORE_PREFIX,
        table_name=DYNAMODB_TABLE_NAME,
        max_threads=WORKER_THREADS
    )
    
    return {
        "statusCode": 200,
        "message": "BLS Sync cycle completed successfully."
    }