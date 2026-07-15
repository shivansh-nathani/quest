# Cloud Data Synchronization & API Ingestion Engine

## 📌 Project Overview
This repository contains enterprise-grade data pipelines designed to ingest government and census data into AWS S3, orchestrate state management, and trigger event-driven analytics. 

* **Part 1:** A multi-threaded synchronization engine that mirrors the Bureau of Labor Statistics (BLS) file directory using DynamoDB for distributed state management.
* **Part 2:** A resilient API ingestion script that streams Data USA JSON payloads directly into S3 while handling complex network edge cases.
* **Part 3:** An event-driven analytics pipeline utilizing SQS and AWS Lambda, executing memory-efficient Pandas transformations on the ingested data.

This document outlines the architectural evolution, the problem-solving methodology, and the process of leveraging AI as a development tool—specifically highlighting where AI logic fell short and required strict engineering oversight, prompt engineering, and debugging by the Engineer to achieve production readiness.

### 🤝 Roles & Collaboration
To clarify the development process for this project:
* **Shivansh (Engineer):** Responsible for problem identification, overarching system design, cost-benefit analysis, security evaluations, and rigorous code auditing. Shivansh utilized advanced prompt engineering to direct the AI, identified its logical blind spots (especially in distributed execution and HTTP protocols), and forced architectural corrections.
* **AI (Pair Programmer):** Utilized to rapidly generate boilerplate code, suggest library implementations (like `boto3` and `urllib3.util.retry`), and format syntax based on Shivansh's explicit prompts.

---

## 🚀 Part 1: BLS Synchronization Engine (Architectural Evolution)

### Phase 1: Local Scraper & Bypassing Defenses
* **The Goal:** Download files locally while bypassing basic bot-blocking policies.
* **The Approach:** Shivansh directed the AI to use built-in Python libraries (`urllib`, `html.parser`) to ensure zero-dependency portability. 
* **Challenge Addressed:** Shivansh bypassed infinite loops caused by Apache/IIS query parameters (e.g., `?C=N;O=D`) by engineering rigid `href` filtering to safely ignore parent directories and dynamic sorting links.

### Phase 2: State Tracking via Local SQLite (Smart Sync)
* **The Goal:** Prevent redundant downloads and manage orphaned files locally to optimize bandwidth.
* **The Approach:** Before moving to the cloud, Shivansh engineered a local synchronization state using a SQLite database. By querying the host server with HTTP `HEAD` requests, Shivansh compared `Last-Modified` timestamps against the SQLite snapshot, reducing bandwidth consumption by over 90%.

### Phase 3: Cloud Migration & Direct S3 Streaming
* **The Problem:** As Shivansh attempted to port the logic to an S3-backed architecture, he identified a critical distributed-systems edge case. If the process crashed midway through execution, new files would stream to S3, but the tracking database would never update. These orphaned files would remain in the bucket forever.
* **The Streaming Directive:** Shivansh explicitly instructed the AI *not* to download files to the local machine before uploading. Instead, he prompted the AI to stream the objects directly into S3 memory buffers to prevent local I/O bottlenecks.
* **The Initial Solution:** To handle the orphaned file edge case, Shivansh proposed a "checkout" mechanism, downloading the state file from S3 to local memory and deleting it from the bucket to act as a mutex lock. 

### Phase 4: Cost Evaluation & DynamoDB Migration
* **The Architectural Review:** Shivansh recognized his S3 lock mechanism introduced severe race conditions and an expensive "nuke penalty" if adapted for multi-threading. During an architectural review, the AI proposed DynamoDB as a concurrent alternative.
* **Cost & Technical Evaluation:** Before adopting the AI's proposal, Shivansh evaluated the AWS pricing model. He determined that configuring the table with **On-Demand Capacity** would keep operational costs at virtually $0.00 per month.
* **The Final Decision:** Shivansh pivoted the architecture to DynamoDB, utilizing the file URL as a Partition Key, completely eliminating file-locking collisions and allowing safe scaling to dozens of concurrent threads.

---

## 🌐 Part 2: Resilient API Ingestion (Data USA)

### Phase 1: Direct Memory Streaming
* **The Goal:** Hit the Data USA API and stream massive JSON payloads directly to S3 without causing local memory bloat.
* **The Approach:** Shivansh prompted the AI to utilize a `requests.Session` with a `urllib3` exponential backoff adapter to handle transient 500-level errors and 429 rate limits. Reusing the architectural directive from Part 1, Shivansh specifically enforced the use of `stream=True` and piped `response.raw` directly into `boto3.client('s3').upload_fileobj()`.

### Phase 2: Protocol Strictness & IAM Delegation
* **The Approach:** Shivansh ensured the system was entirely credential-less locally by relying on the machine's attached IAM roles, rejecting the AI's initial assumptions about passing S3 keys. Furthermore, Shivansh audited the AI's HTTP validation logic and identified a major flaw in how it handled success responses, which he manually corrected (detailed below).

---

## 🏗️ Part 3: Event-Driven Analytics & Infrastructure (AWS CDK)

### Phase 1: Spark Migration & Serverless Analytics
* **The Goal:** Perform complex transformations and aggregations on the ingested JSON data without incurring the heavy infrastructure costs and startup times associated with AWS Glue or EMR clusters.
* **The Approach:** Shivansh audited the legacy PySpark analytics codebase and orchestrated a complete port to native Python using the Pandas library. By converting the Spark DAGs into memory-efficient Pandas logic, the analytics engine was successfully decoupled from heavy data-processing clusters and refactored to run entirely within a lightweight, serverless AWS Lambda function.

### Phase 2: Event-Driven Orchestration (S3 → SQS → Lambda)
* **The Goal:** Automate the execution of the Pandas data analysis script strictly when a new Data USA JSON file lands in a specific S3 directory.
* **The Approach:** Shivansh designed an event-driven architecture using AWS CDK. S3 Event Notifications detect new payloads in `data/datausa/` and push notifications to an Amazon SQS Queue. SQS securely buffers the events and invokes the Pandas analytics Lambda. This decoupled design ensures the ingestion script (Part 2) requires zero code changes to integrate into the broader pipeline.

### Phase 3: Legacy Resource Integration & Migration
* **The Goal:** Deploy the automated CDK pipeline using an S3 bucket and DynamoDB table that were already manually provisioned, and migrate the stack to the `eu-north-1` region.
* **The Approach:** Shivansh refactored the CDK infrastructure code to use `.from_bucket_name()` and `.from_table_name()`. This transformed the CDK script from a resource *creator* to a resource *importer*, avoiding `BucketAlreadyExists` stack collisions. A new regional `cdk bootstrap` was executed, bridging the existing data layer with the new automated compute layer seamlessly.

---

## 🐛 AI Oversights & Engineer Corrections

While the AI was highly effective at generating boilerplate, it routinely missed critical systems-level edge cases. Below are the specific scenarios where Shivansh audited the AI's output, diagnosed the flaws, and engineered prompts to fix them.

### 1. The Local Caching Anti-Pattern (Parts 1 & 2)
* **The AI's Mistake:** When asked to move files to S3, the AI natively defaulted to a legacy two-step pattern: downloading the file to the local disk, and then uploading that local file to S3. 
* **The Correction:** Shivansh recognized this would cause severe disk I/O bottlenecks, exhaust local storage, and bloat RAM when handling massive census JSONs. He strictly prompted the AI to abandon local caching and refactor the architecture to stream the HTTP network buffer directly into AWS via `upload_fileobj`, bypassing the local disk entirely.

### 2. Resolving the "Pristine Run" Orphan Race Condition (Part 1)
* **The AI's Mistake:** The AI wrote the DynamoDB cleanup logic *after* the multi-threaded upload phase. If an S3 bucket already had legacy files, but the DynamoDB table was empty, the worker threads would populate the table *before* the cleanup phase executed. The cleanup phase would see a populated table, skip the empty-state safeguard, and leave S3 orphans permanently.
* **The Correction:** Shivansh traced the execution lifecycle and identified the race condition. He instructed the AI to re-architect the engine loop so that Phase 1 explicitly pulls the DynamoDB state *before* host scanning begins, guaranteeing a prefix-wide purge if the state is genuinely empty.

### 3. The HTTP 406 Protocol Truncation (Part 1)
* **The AI's Mistake:** During the transition to multi-threading, the AI inadvertently truncated Shivansh's carefully crafted `Accept` header to just `text/html,application/xml`. When the threads attempted to download raw TSV/Data files, the BLS host server rejected the requests with HTTP 406 Not Acceptable.
* **The Correction:** Shivansh diagnosed the 406 error as a content negotiation failure and prompted the AI to restore the open wildcard (`*/*`) to the `Accept` header, satisfying the server's strict requirements.

### 4. The Silent Execution Bug (Part 1)
* **The AI's Mistake:** When implementing the `ThreadPoolExecutor`, the AI commented out the console print statements that yielded thread results, causing the script to fail silently on the first run if AWS DynamoDB permissions were missing.
* **The Correction:** Shivansh audited the thread pool logic, identified the swallowed outputs, and directed the AI to implement a comprehensive "Pre-Flight IAM Check" to explicitly validate table access before spinning up threads.

### 5. The Broad HTTP 2xx Acceptance Flaw (Part 2)
* **The AI's Mistake:** For the API ingestion script, the AI used `response.raise_for_status()` as the validation guardrail before uploading to S3. 
* **The Correction:** Shivansh recognized that `raise_for_status()` broadly accepts *any* 2xx code. If the API returned a `204 No Content` or `206 Partial Content`, the script would happily stream incomplete or empty data to S3. Shivansh prompted the AI to remove the native function and enforce a strict, exact integer match for `200 OK` to guarantee data payload integrity.

### 6. The Hardcoded Resource Collision (CDK)
* **The AI's Mistake:** The AI generated CDK boilerplate utilizing `s3.Bucket(...)` constructors, attempting to mint brand new resources for an infrastructure that already existed manually.
* **The Correction:** Shivansh recognized this would trigger an immediate CloudFormation rollback. He explicitly directed the AI to utilize static import methods (`.from_bucket_name()`) to safely wrap the existing data layer without collision.

---

## ⚙️ Final System Architecture

The final production scripts utilize the following architectures designed by Shivansh:

**The Sync Engine (Part 1):**
1.  **State Snapshot:** Pulls the complete DynamoDB inventory into memory to evaluate state and trigger pre-emptive purges if necessary.
2.  **Synchronous Host Discovery:** Scans the target host on a single thread to map the file tree without triggering rate-limiting.
3.  **Thread Pool Execution:** Spawns `concurrent.futures` workers to check DynamoDB timestamp parity, stream updates to S3, and upsert success states.
4.  **Reconciliation Phase:** Cross-references the active host map against the initial DynamoDB snapshot, pruning orphaned files.

**The API Ingestion Script (Part 2):**
1.  **Resilient Session:** Utilizes `urllib3` adapters to automatically manage exponential backoffs for 429 and 5xx API errors.
2.  **Strict Validation:** Enforces a rigid `200 OK` check before initializing S3 connectivity.
3.  **Memory-Safe Streaming:** Pipes the raw network buffer directly into AWS via `upload_fileobj`, utilizing zero local disk space and negligible RAM.

**The Event-Driven Analytics Pipeline (Part 3):**
1. **The Catalyst:** The Part 2 ingestion script successfully writes a JSON payload to the specific S3 prefix (`data/datausa/`).
2. **The Invisible Handoff (S3 Event Notification):** S3 natively detects the `OBJECT_CREATED` event and constructs a notification payload without any compute overhead.
3. **The Message Broker (SQS Queue):** The S3 payload is securely queued in an Amazon SQS buffer configured with a 6-minute visibility timeout to prevent duplicate processing.
4. **The Analytics Engine (Lambda + Pandas):** SQS triggers a memory-optimized Lambda function (1024MB) augmented by an AWS Managed Pandas Layer. The inline Python script pulls the target file from S3, executes the Pandas transformations natively (bypassing AWS Glue), and gracefully terminates.

---
*💡 **Note:** Architecture and thoughts are my own, articulated by AI. Vibe-coded with AI.*