import os
import urllib.request
from urllib.parse import urljoin, unquote
from html.parser import HTMLParser
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        with urllib.request.urlopen(req) as response:
            html_content = response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"[!] Failed to access {url}: {e}")
        return []

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
    """Worker function executed by the ThreadPoolExecutor."""
    file_url, rel_path = file_info
    s3_key = f"{s3_prefix}{rel_path}".replace('\\', '/')
    
    session = boto3.Session()
    s3 = session.client('s3')
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)

    head_req = urllib.request.Request(file_url, headers=headers, method='HEAD')
    try:
        with urllib.request.urlopen(head_req) as response:
            server_modified = response.headers.get('Last-Modified', '')
    except Exception as e:
        return f"[!] Failed host headers for {rel_path}: {e}"

    try:
        db_response = table.get_item(Key={'url': file_url})
        db_item = db_response.get('Item')
    except ClientError as e:
        return f"[!] DynamoDB read error for {rel_path}: {e}"

    if not db_item or db_item.get('last_modified') != server_modified:
        get_req = urllib.request.Request(file_url, headers=headers)
        try:
            with urllib.request.urlopen(get_req) as response:
                s3.upload_fileobj(response, bucket_name, s3_key)
            
            table.put_item(Item={
                'url': file_url,
                's3_key': s3_key,
                'last_modified': server_modified
            })
            return f"[+] Success (New/Updated): {rel_path} -> s3://{bucket_name}/{s3_key}"
        except Exception as e:
            return f"[!] Upload failed for {rel_path}: {e}"
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
        print(f"[!] FATAL: Cannot connect to DynamoDB table '{table_name}'. Confirm it exists.")
        print(f"Error Details: {e}")
        return

    # 1. Pull the state FIRST to ensure accurate evaluations
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
        print(f"[!] Failed to pull state tracking inventory from DynamoDB: {e}")
        return

    # 2. Immediately execute the safeguard if the table is empty
    is_pristine_run = False
    if not db_items:
        print("[-] DynamoDB state table is empty. Purging target S3 path to guarantee clean synchronization...")
        try:
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix):
                if 'Contents' in page:
                    delete_keys = [{'Key': obj['Key']} for obj in page['Contents']]
                    s3.delete_objects(Bucket=bucket_name, Delete={'Objects': delete_keys})
            print("[*] S3 target path cleared successfully.")
            is_pristine_run = True
        except Exception as e:
            print(f"[!] Error while cleaning target S3 path: {e}")
            return
    else:
        print(f"[*] Loaded {len(db_items)} state records from DynamoDB.")

    # 3. Safe to scan the host server now
    print("\n--- Phase 2: Scanning Host Server ---")
    remote_files = get_remote_files(base_url, headers)
    remote_urls = set([f[0] for f in remote_files])

    if not remote_files:
        print("[!] No remote files discovered on the target server. Aborting.")
        return

    # 4. Stream to S3
    print(f"\n--- Phase 3: Multi-Threaded Sync ({max_threads} workers) ---")
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_url = {
            executor.submit(
                process_single_file, file_info, headers, bucket_name, s3_prefix, table_name
            ): file_info for file_info in remote_files
        }
        
        for future in as_completed(future_to_url):
            result_msg = future.result()
            print(result_msg)

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
    else:
        print("\n--- Phase 4: Orphan Cleanup (Skipped - Pristine Run) ---")

    print("\n[*] Multi-Threaded Cloud Sync Engine Cycle Complete.")

def handler(event, context):
    """
    AWS Lambda Entry Point
    """
    print("[-] Starting BLS Data Sync Lambda...")
    
    # Configuration
    TARGET_URL = "https://download.bls.gov/pub/time.series/pr/" 
    HEADERS = {
        'User-Agent': 'hello@gmail.com',
        'Sec-Ch-Ua-Platform': '"Linux"'
    }


    S3_BUCKET_NAME = os.environ['BUCKET_NAME']
    S3_STORE_PREFIX = "data/bls_data"          
    WORKER_THREADS = 2 
    DYNAMODB_TABLE_NAME = os.environ.get('TABLE_NAME')

    # Trigger the main engine
    try:
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
        
    except Exception as e:
        print(f"[!] Fatal error in Lambda execution: {e}")
        # Raise the exception so AWS Step Functions knows this step failed
        raise e