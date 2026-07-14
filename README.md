# BLS Data Synchronization Engine

## 📌 Project Overview
This project is a resilient, multi-threaded synchronization engine designed to mirror government datasets (specifically the Bureau of Labor Statistics) to an AWS S3 bucket. 

The architecture evolved from a simple local web scraper into a distributed, cloud-native synchronization pipeline. This document outlines the problem-solving methodology, the identification and resolution of critical distributed-systems edge cases, and the cost-benefit analysis that drove the final architectural decisions.

### 🤝 Roles & Collaboration
To clarify the development process for this project:
* **Shivansh (Lead Engineer):** Responsible for problem identification, architectural direction, cost-benefit analysis, security evaluations, and final implementation decisions.
* **AI (Pair Programmer):** Utilized for architectural review, stress-testing edge cases, and rapidly iterating on boilerplate syntax.

---

## 🚀 Architectural Evolution & Engineering Process

### Phase 1: Local Scraper & Bypassing Defenses
* **The Goal:** Download files locally while bypassing basic bot-blocking policies.
* **The Approach:** Shivansh utilized built-in Python libraries (`urllib`, `html.parser`) to ensure portability without relying on external dependencies like `requests`. 
* **Challenge Addressed:** Shivansh bypassed infinite loops caused by Apache/IIS query parameters (e.g., `?C=N;O=D`) by implementing rigid `href` filtering to safely ignore parent directories and dynamic sorting links.

### Phase 2: State Tracking via Local SQLite (Smart Sync)
* **The Goal:** Prevent redundant downloads and manage orphaned files locally to optimize bandwidth.
* **The Approach:** Before moving to the cloud, Shivansh engineered a local synchronization state using a SQLite database. Instead of blindly re-downloading files, the script queried the host server with HTTP `HEAD` requests to check `Last-Modified` timestamps, comparing them against the SQLite snapshot.
* **Result:** This optimization reduced bandwidth consumption and execution time by over 90% during routine daily syncs.

### Phase 3: Cloud Migration & Identifying the "Stale State"
* **The Goal:** Migrate storage to AWS S3 without caching files to local disk.
* **The Problem (The "Dirty/Stale State"):** As Shivansh attempted to port the SQLite logic to an S3-backed architecture, he identified a critical distributed-systems edge case. If the process crashed midway through an execution cycle, new files would be successfully streamed to S3, but the tracking database would never receive the update (resulting in a stale state). These orphaned files would remain in the bucket forever, invisible to the cleanup phase.
* **The Initial Solution:** Shivansh proposed and designed a "checkout" mechanism. The script would download the state file from S3 to local memory and immediately delete it from the bucket, effectively treating the file as a mutex lock. If the process crashed, the lock would be dropped, and the next run would detect the missing state and trigger a pristine, full-sync recovery.

### Phase 4: Cost Evaluation & DynamoDB Migration
* **The Goal:** Enable safe, concurrent multi-threading while maintaining the integrity of the state tracker.
* **The Architectural Review:** While Shivansh's S3 lock mechanism solved the stale state for a single thread, he recognized it introduced severe race conditions and an expensive "nuke penalty" (re-downloading the entire dataset) if adapted for multi-threading. During a collaborative architectural review, the AI proposed DynamoDB as a concurrent alternative.
* **Cost & Technical Evaluation:** Before adopting the AI's proposal, Shivansh evaluated the AWS pricing model and operational overhead. Shivansh determined that configuring the table with **On-Demand Capacity** (rather than Provisioned) would keep the operational cost at virtually $0.00 per month, as the required read/write volume fell well within the AWS Free Tier. 
* **The Final Decision:** Shivansh pivoted the architecture to DynamoDB, utilizing the file URL as a Partition Key. This completely eliminated file-locking collisions, provided perfect idempotency, and allowed the system to scale safely to dozens of concurrent threads.

---

## 🐛 Key Debugging Scenarios & Resolutions

During development, several silent failures and protocol-level errors were identified and resolved.

### 1. The IIS Absolute Path Silent Failure
* **Issue:** The initial HTML parser found zero valid links, finishing execution silently.
* **Diagnosis:** The BLS server (running IIS) generated absolute paths in its `<a>` tags rather than relative ones. Shivansh's initial security filter aggressively dropped anything starting with `/`.
* **Resolution:** Shivansh refactored the path resolution logic using `urljoin` to dynamically check if the resulting absolute URL remained safely within the target directory tree, restoring full parsing capabilities.

### 2. The HTTP 406 Not Acceptable Error
* **Issue:** S3 streams failed immediately upon executing the multi-threaded `urllib.request.urlopen()`.
* **Diagnosis:** During the transition to multi-threading, the `Accept` header was inadvertently truncated to `text/html,application/xml`. When the threads attempted to download raw TSV/Data files, the BLS host server rejected the requests because the file formats did not match the strict HTML/XML types.
* **Resolution:** Shivansh reintroduced the open content negotiation wildcard (`*/*`) to the `Accept` header, satisfying the server's protocol requirements.

### 3. Resolving the "Pristine Run" Orphan Race Condition
* **Issue:** If the S3 bucket already contained files from a legacy run, but the DynamoDB table was brand new or wiped, the cleanup function would skip execution, leaving S3 orphans permanently.
* **Diagnosis:** Shivansh traced the execution order and found that pulling the DynamoDB state *after* the worker threads had populated it masked the true "empty" state of the table. 
* **Resolution:** Shivansh re-architected the main engine loop. Phase 1 now explicitly pulls the DynamoDB state *before* any host scanning begins. If the state is entirely empty, it triggers an immediate, prefix-wide S3 purge, guaranteeing absolute environment parity before the threads spin up.

---

## ⚙️ Final System Architecture

The final production script utilizes the following architecture designed by Shivansh:

1.  **Pre-Flight IAM Check:** Validates AWS credentials and DynamoDB access before initializing any operations.
2.  **State Snapshot:** Pulls the complete DynamoDB inventory into memory to evaluate state and trigger pre-emptive purges if necessary.
3.  **Synchronous Host Discovery:** Scans the target host iteratively on a single thread to map the file tree without triggering DDoS protection or rate-limiting.
4.  **Thread Pool Execution:** Spawns `concurrent.futures.ThreadPoolExecutor` workers. Each thread:
    * Initializes a thread-safe `boto3.Session`.
    * Checks the DynamoDB state for timestamp parity.
    * Streams the file directly from HTTP to S3.
    * Upserts the success state to DynamoDB.
5.  **Reconciliation Phase:** Cross-references the active host map against the initial DynamoDB snapshot, automatically pruning orphaned files from both S3 and the database.