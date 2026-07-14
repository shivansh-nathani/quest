import os
import sqlite3
import urllib.request
from urllib.parse import urljoin, unquote
from html.parser import HTMLParser

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
    """Recursively scans the directory and returns a list of (url, relative_path)."""
    url = base_url + current_rel_dir
    print(f"[*] Scanning: {url}")
    
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
        # Skip empty links, parent directories, or query parameters
        if not href or href in ('../', '..') or href.startswith('?'):
            continue
            
        full_url = urljoin(url, href)
        
        # Ensure we stay inside the target directory structure
        if not full_url.startswith(url) or full_url == url:
            continue
            
        # Extract the exact file or directory name
        name = unquote(full_url.rstrip('/').split('/')[-1])
        
        if href.endswith('/'):
            # Recursively append files from sub-directories
            files.extend(get_remote_files(base_url, headers, current_rel_dir + name + '/'))
        else:
            files.append((full_url, current_rel_dir + name))
            
    return files

def sync_directory(base_url, local_dir, db_path, headers):
    """Synchronizes local files with the server using SQLite for state tracking."""
    if not base_url.endswith('/'):
        base_url += '/'
        
    os.makedirs(local_dir, exist_ok=True)

    # 1. Initialize SQLite Database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS files 
                 (url TEXT PRIMARY KEY, local_path TEXT, last_modified TEXT)''')
    conn.commit()

    # 2. Get list of all current files on the server
    print("\n--- Phase 1: Scanning Server ---")
    remote_files = get_remote_files(base_url, headers)
    remote_urls = set([f[0] for f in remote_files])

    if not remote_files:
        print("[!] No files found. Check URL or headers.")
        conn.close()
        return

    # 3. Download new or updated files
    print("\n--- Phase 2: Syncing Files ---")
    for file_url, rel_path in remote_files:
        # Normalize path to ensure correct slashes for Windows/Mac/Linux
        local_path = os.path.normpath(os.path.join(local_dir, rel_path))
        
        # Ask the server for the file's Last-Modified date using an HTTP HEAD request
        req = urllib.request.Request(file_url, headers=headers, method='HEAD')
        try:
            with urllib.request.urlopen(req) as response:
                server_modified = response.headers.get('Last-Modified', '')
                print(f"[*] Fetched headers for {file_url}: Last-Modified = {server_modified}")
        except Exception as e:
            print(f"[!] Failed to fetch headers for {file_url}: {e}")
            continue

        # Check our database for the last known modified date
        cursor.execute('SELECT last_modified FROM files WHERE url = ?', (file_url,))
        row = cursor.fetchone()
        
        # Trigger download if: Not in DB OR Date changed OR Local file was manually deleted
        if row is None or row[0] != server_modified or not os.path.exists(local_path):
            print(f"[+] Downloading (New/Updated): {rel_path}")
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            get_req = urllib.request.Request(file_url, headers=headers)
            try:
                with urllib.request.urlopen(get_req) as response, open(local_path, 'wb') as out_file:
                    while True:
                        chunk = response.read(8192)
                        if not chunk:
                            break
                        out_file.write(chunk)
                        
                # Upsert into the database ONLY after a successful download
                cursor.execute('''INSERT OR REPLACE INTO files (url, local_path, last_modified) 
                             VALUES (?, ?, ?)''', (file_url, local_path, server_modified))
                conn.commit()
            except Exception as e:
                print(f"[!] Failed to download {rel_path}: {e}")
                if os.path.exists(local_path):
                    os.remove(local_path)
        else:
            print(f"[-] Skipped (Up to date): {rel_path}")

    # 4. Cleanup local files that no longer exist on the server
    print("\n--- Phase 3: Cleanup ---")
    cursor.execute('SELECT url, local_path FROM files')
    db_records = cursor.fetchall()
    
    for db_url, db_local_path in db_records:
        if db_url not in remote_urls:
            print(f"[x] Deleting missing file: {db_local_path}")
            if os.path.exists(db_local_path):
                os.remove(db_local_path)
            cursor.execute('DELETE FROM files WHERE url = ?', (db_url,))
            conn.commit()

    conn.close()
    print("\n[*] Sync Complete.")

if __name__ == '__main__':
    # Configuration
    TARGET_URL = "https://download.bls.gov/pub/time.series/pr/" 
    LOCAL_DESTINATION = "./bls_files" 
    DB_FILE = "sync_state.db"
    
    # Headers without Accept-Encoding to prevent gzip binary errors
    HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Cookie': 'nmstat=998b73d4-ac17-51da-a2f8-0d658e3cd772',
        'Priority': 'u=0, i',
        'Referer': 'https://github.com/rearc-data/quest',
        'Sec-CH-UA': '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
        'Sec-CH-UA-Mobile': '?0',
        'Sec-CH-UA-Platform': '"macOS"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36'
    }

    sync_directory(TARGET_URL, LOCAL_DESTINATION, DB_FILE, HEADERS)