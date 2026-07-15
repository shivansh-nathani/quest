import os
import sys
import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError

def create_resilient_session(retries=3, backoff_factor=1.0):
    """
    Creates a requests Session with automatic exponential backoff 
    for transient network errors and rate limits.
    """
    session = requests.Session()
    
    retry_strategy = Retry(
        total=retries,
        read=retries,
        connect=retries,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=backoff_factor,
        allowed_methods=["GET"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session

def stream_api_to_s3(api_url, bucket_name, destination_path, timeout=15):
    """
    Fetches data from an API and streams it directly to S3.
    Strictly requires an HTTP 200 OK response.
    """
    session = create_resilient_session()
    
    # Boto3 automatically uses the machine's attached IAM roles or local credentials
    s3_client = boto3.client('s3')

    print(f"[*] Initiating connection to API: {api_url}")
    
    try:
        # stream=True prevents loading the entire JSON payload into local memory
        with session.get(api_url, stream=True, timeout=timeout) as response:
            
            # --- STRICT 200 OK ENFORCEMENT ---
            if response.status_code != 200:
                print(f"[!] API returned {response.status_code} {response.reason}. Only 200 OK is permitted. Aborting.")
                sys.exit(1)
            
            print(f"[*] Connection established (HTTP 200). Streaming data to s3://{bucket_name}/{destination_path}")
            
            # Ensures transparent decompression if the API sends gzip payloads
            response.raw.decode_content = True 
            
            # Pipe the raw network response directly into the S3 bucket
            s3_client.upload_fileobj(response.raw, bucket_name, destination_path)
            print("[+] Upload completed successfully.")

    # --- Comprehensive Edge Case Handling ---
    except requests.exceptions.ConnectionError as conn_err:
        print(f"[!] Connection Error: Failed to establish a connection to the API.\nDetails: {conn_err}")
        sys.exit(1)
        
    except requests.exceptions.Timeout as timeout_err:
        print(f"[!] Timeout Error: The API took too long to respond ({timeout}s).\nDetails: {timeout_err}")
        sys.exit(1)
        
    except requests.exceptions.RequestException as req_err:
        print(f"[!] Request Error: An unexpected network error occurred.\nDetails: {req_err}")
        sys.exit(1)
        
    except ClientError as aws_err:
        print(f"[!] AWS S3 Error: Failed to stream to S3. Check IAM permissions and bucket name.\nDetails: {aws_err}")
        sys.exit(1)
        
    except Exception as e:
        print(f"[!] Fatal Error: An unexpected application fault occurred.\nDetails: {e}")
        sys.exit(1)


def handler(event, context):
    print("[-] Starting DataUSA Stream Lambda...")
    TARGET_API_URL = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    
    # Pull bucket dynamically from CDK
    TARGET_BUCKET = os.environ['BUCKET_NAME']
    DESTINATION_FILE_PATH = "data/datausa/population_data.json" 
    
    stream_api_to_s3(TARGET_API_URL, TARGET_BUCKET, DESTINATION_FILE_PATH)
    
    return {"statusCode": 200, "message": "Data successfully streamed to S3"}