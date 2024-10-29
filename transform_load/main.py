import functions_framework
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage

# Helper function to check if a specific record already exists in BigQuery
def record_exists(bq_client, dataset_name, table_name, country, year_month):
    """
    Checks if a record for the given country and year_month already exists in BigQuery.
    - bq_client: BigQuery client
    - dataset_name: Name of the dataset in BigQuery
    - table_name: Name of the table in BigQuery
    - country: Country name to check for
    - year_month: Date to check for in 'YYYY-MM-DD' format
    """
    query = f"""
        SELECT COUNT(*) as count
        FROM `{dataset_name}.{table_name}`
        WHERE country = @country AND year_month = @year_month
    """
    
    # Configuring the query to accept parameters for security and readability
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("country", "STRING", country),
            bigquery.ScalarQueryParameter("year_month", "DATE", year_month)
        ]
    )
    
    # Execute the query and obtain the result
    query_job = bq_client.query(query, job_config=job_config)
    result = query_job.result()
    row = next(result)
    
    # Return True if the record already exists, otherwise False
    return row.count > 0

# Main Cloud Function to load data from GCS into BigQuery
@functions_framework.cloud_event
def load_data_to_bq(data, cloud_event):
    """
    Cloud Function triggered by a cloud event to load JSON data from Google Cloud Storage into BigQuery.
    - Checks for duplicate records before insertion to prevent redundant data.
    """
    # Initialize Google Cloud Storage and BigQuery clients
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Define BigQuery dataset and table names
    dataset_name = 'agriculture_production'
    table_name = 'tbl_production'

    # Extract file information from the cloud event trigger
    data = cloud_event.data
    bucket_name = data['bucket']
    file_name = data['name']

    # Load and parse JSON data from the specified GCS bucket and file
    blob = storage_client.get_bucket(bucket_name).blob(file_name)
    json_data = json.loads(blob.download_as_string())

    # List to store rows that are ready to be inserted into BigQuery
    transformed_rows = []

    # Iterate through each record in the JSON data and transform it for BigQuery insertion
    for item in json_data:
        # Extract fields from the JSON object
        country = item['memberStateName']
        year = item['year']
        month = item['month']
        category = item['category']
        tonnes = item['tonnes']

        # Convert year and month to a 'YYYY-MM-DD' date format for consistency
        month_num = datetime.strptime(month, '%B').month
        year_month = f"{year}-{month_num:02d}-01"

        # Check if the record already exists in BigQuery to avoid duplicates
        if record_exists(bq_client, dataset_name, table_name, country, year_month):
            print(f"Record for {country} in {year_month} already exists. Skipping.")
            continue

        # Prepare the transformed row in a dictionary format for BigQuery insertion
        transformed_row = {
            "country": country,
            "year_month": year_month,
            "tonnes": tonnes,
            "category": category
        }
        transformed_rows.append(transformed_row)

    # Insert only if there are new records
    if transformed_rows:
        table_ref = bq_client.dataset(dataset_name).table(table_name)
        errors = bq_client.insert_rows_json(table_ref, transformed_rows)
        
        # Confirm insertion success or print errors if any occurred
        if errors == []:
            print("Data loaded successfully.")
        else:
            print(f"Errors occurred while loading data: {errors}")
    else:
        print("No new data to load.")