import io
import json
import boto3
import pandas as pd
import os
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    1. Trims and normalizes all column headers (lowercase, stripped, spaces to underscores).
    2. Trims leading/trailing whitespace from string/text columns.
    """
    # Step 1: Clean column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    
    # Step 2: Clean text cell content for string columns
    for col in df.columns:
        if df[col].dtype == 'object' or isinstance(df[col].dtype, pd.StringDtype):
            # Check if elements are strings before applying strip
            df[col] = df[col].astype(str).str.strip()
            
    return df

def run_analysis():
    s3_client = boto3.client('s3')
    bucket_name = os.environ['BUCKET_NAME']
    
    print("--- Phase 1: Processing BLS Time Series Data ---")
    
    # Read the tab-separated BLS file from S3 into memory
    bls_key = "data/bls_data/pr.data.0.Current"
    bls_obj = s3_client.get_object(Bucket=bucket_name, Key=bls_key)
    
    # read_csv handles the tab delimiter and schema inference automatically
    pr_df = pd.read_csv(io.BytesIO(bls_obj['Body'].read()), sep='\t')
    
    # Clean the dataframe columns and contents
    pr_cleaned_df = clean_dataframe(pr_df)
    
    # Log column schemas
    for col, dtype in zip(pr_cleaned_df.columns, pr_cleaned_df.dtypes):
        print(f"Column: {col}, Data Type: {dtype}")
        
    
    # Filter for the specific series ID and period
    filtered_ts = pr_cleaned_df[
        (pr_cleaned_df['series_id'] == "PRS30006032") & 
        (pr_cleaned_df['period'] == "Q01")
    ].copy()
    
    
    
    
    
    print("\n--- Phase 1: Processing DataUSA Population Data ---")
    
    # Read the population JSON file from S3
    pop_key = "data/datausa/population_data.json"
    pop_obj = s3_client.get_object(Bucket=bucket_name, Key=pop_key)
    raw_json = json.loads(pop_obj['Body'].read().decode('utf-8'))
    
    # Flatten the struct array wrapped in the 'data' key (mimicking Spark's explode)
    final_df = pd.DataFrame(raw_json['data'])
    
    # Normalize data types: Cast Population to Integer, ensure Year is numeric matching BLS
    final_df['Population'] = final_df['Population'].astype(int)
    final_df['Year'] = pd.to_numeric(final_df['Year'])
    
    # Run structural cleaning step
    population_df = clean_dataframe(final_df)
    
    # Filter population metrics between 2013 and 2018 inclusive
    filtered_population_df = population_df[population_df['year'].between(2013, 2018)]
    
    # Calculate aggregate summary stats (Mean and Standard Deviation)
    mean_pop = filtered_population_df['population'].mean()
    std_pop = filtered_population_df['population'].std()
    
    # Format metrics cleanly with commas and two decimal places
    print("\n[Population Stats (2013 - 2018)]:")
    print(f"Mean Population:   {mean_pop:,.2f}")
    print(f"StdDev Population: {std_pop:,.2f}")
    

    # Part 2 Report: Find the year with the maximum total value per series_id
    # 1. Group by series_id and year, summing the values
    yearly_sum_df = pr_cleaned_df.groupby(['series_id', 'year'], as_index=False)['value'].sum()
    
    # 2. Sort values to replicate window function ordering (value descending)
    yearly_sum_df = yearly_sum_df.sort_values(by=['series_id', 'value'], ascending=[True, False])
    
    # 3. Deduplicate by series_id keeping the first match (highest value due to sort) to get rank == 1
    report_df_1 = yearly_sum_df.drop_duplicates(subset=['series_id'], keep='first')
    
    print("\n[Highest Total Value Year Per Series]:")
    print(report_df_1.head(20000).to_string(index=False))



    
    print("\n--- Phase 3: Joining Datasets ---")
    
    # Ensure both dataframes share identical data types on the join key
    filtered_ts['year'] = filtered_ts['year'].astype(int)
    population_df['year'] = population_df['year'].astype(int)
    
    # Perform inner merge on matching 'year' fields
    joined_report_df = pd.merge(
        filtered_ts,
        population_df,
        on="year",
        how="inner"
    )
    
    print("\n[Final Merged Report Output]:")
    print(joined_report_df[['series_id','year','period', 'value', 'population']].to_string(index=False))
    
    # Option to push output back to S3 as an analytical CSV asset
    # csv_buffer = io.StringIO()
    # joined_report_df.to_csv(csv_buffer, index=False)
    # s3_client.put_object(Bucket=bucket_name, Key="data/reports/final_analysis_output.csv", Body=csv_buffer.getvalue())

def handler(event, context):
    """
    This is triggered by the SQS Queue.
    """
    try:
        # Check if the event came from SQS
        records = event.get('Records', [])
        
        if not records:
            print("[-] Manual invocation detected. Running analysis...")
            run_analysis()
        else:
            print(f"[-] Detected {len(records)} message(s) in the SQS Queue.")
            for record in records:
                # Log the SQS message ID for tracking
                message_id = record.get('messageId', 'Unknown')
                print(f"[*] Processing SQS Message ID: {message_id}")
                
                # Execute the Pandas reporting logic
                # (This will log all the dataframes to CloudWatch as required)
                run_analysis()
                
                print(f"[+] Successfully processed message ID: {message_id}")

        return {
            "statusCode": 200,
            "body": "Analysis pipeline executed successfully via SQS."
        }
    except Exception as e:
        print(f"[!] Analysis pipeline failed: {e}")
        raise e