# Import essential libraries for Google Cloud Function
import functions_framework
import requests
from datetime import datetime
from google.cloud import storage
import json

# Main function for fetching agricultural production data and storing it in Google Cloud Storage
@functions_framework.http
def fetch_data(request):
    
    # Define the target Google Cloud Storage (GCS) bucket where data will be saved
    bucket_name = 'agridata_project_bucket'
    bucket = storage.Client().get_bucket(bucket_name)

    # Set parameters for the API request to control which data is retrieved
    year = datetime.now().year              # Current year
    month = datetime.now().month - 1        # Previous month (adjust for testing)
    categories = 'Bull,Bullock'             # Specific animal categories to filter by
    country = 'LU'                          # Country code for Luxembourg

    # Construct the API endpoint URL with the chosen parameters
    URL = f'https://www.ec.europa.eu/agrifood/api/beef/production?\
    memberStateCodes={country}&years={year}&categories={categories}&months={month}'

    # Send the GET request to the API and parse the JSON response
    response = requests.get(URL)
    response.raise_for_status()             # Raise an error if the request fails
    json_data = response.json()             # Parse the response as JSON data

    # Store the fetched data in the specified GCS bucket

    ## Create a unique file name based on the current timestamp to avoid overwrites
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    blob = bucket.blob(f'{timestamp}.json')

    ## Upload the JSON data as a file to GCS
    blob.upload_from_string(
        data=json.dumps(json_data),
        content_type='application/json'
    )

    # Return a success message upon completion
    return 'Data fetched and stored successfully'