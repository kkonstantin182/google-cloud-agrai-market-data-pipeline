import base64
import logging
import os
from email.mime.text import MIMEText
import functions_framework
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.cloud import bigquery

# Utility function to refresh OAuth 2.0 credentials if they have expired
def refresh_credentials(credentials):
    """
    Refreshes OAuth 2.0 credentials if they are expired, allowing the function to 
    continue sending emails with refreshed access tokens.
    """
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())

# Function to query the BigQuery table and retrieve relevant production data
def fetch_data_from_bigquery():
   
    """
    Queries BigQuery to retrieve 'Bull' production data for Luxembourg.
    Returns a list of rows, each containing production data.
    """
    client = bigquery.Client()
    query = """
        SELECT year_month, country, tonnes, category
        FROM `abiding-aspect-439119-p6.agriculture_production.tbl_production`
        WHERE country = 'Luxembourg' AND category = 'Bull'
        ORDER BY year_month ASC
    """
    # Execute query and fetch results
    query_job = client.query(query)
    results = query_job.result()
    
    # Convert results into a list of row objects for processing
    return [row for row in results]

# Function to format the email content using HTML for a structured message
def format_email_content(data):
    """
    Formats the fetched BigQuery data into an HTML email message.
    - Adds headers for meat type and country.
    - Iterates through each row to include monthly production details.
    """
    meat_type = "Bull"
    country = "Luxembourg"
    email_body = f"<b>{meat_type}</b><br><b>{country} production</b><br><br>"

    # Add each data entry as a separate line in the email
    for row in data:
        year_month = row.year_month.strftime('%B %Y')
        tonnes = row.tonnes
        email_body += f"In the month of <b>{year_month}</b>, the {meat_type} production was <b>{tonnes} 1000 tonnes</b>.<br>"

    return email_body

# Main Cloud Function to authenticate and send an email with BigQuery data
@functions_framework.http
def send_email(request):
    """
    Triggered by a request, this function:
    1. Loads OAuth 2.0 credentials from environment variables.
    2. Refreshes the credentials if needed.
    3. Queries BigQuery for production data.
    4. Formats and sends an email with the retrieved data.
    """
    logging.info("Received event from BigQuery to send email.")

    # Load OAuth 2.0 credentials from environment variables
    token_info = {
        "token": os.environ.get("ACCESS_TOKEN"),
        "refresh_token": os.environ.get("REFRESH_TOKEN"),
        "token_uri": "https://oauth2.googleapis.com/token",
        "client_id": os.environ.get("CLIENT_ID"),
        "client_secret": os.environ.get("CLIENT_SECRET")
    }
    credentials = Credentials(
        token=token_info['token'],
        refresh_token=token_info['refresh_token'],
        token_uri=token_info['token_uri'],
        client_id=token_info['client_id'],
        client_secret=token_info['client_secret']
    )

    # Refresh credentials if they have expired to ensure authentication
    refresh_credentials(credentials)
    logging.info("Credentials refreshed successfully.")

    # Fetch relevant production data from BigQuery
    data = fetch_data_from_bigquery()
    if not data:
        logging.info("No data found for the specified filters.")
        return "No data found to send."

    # Format the data into an HTML email message
    email_content = format_email_content(data)

    # Initialize Gmail API service with refreshed credentials
    service = build('gmail', 'v1', credentials=credentials)
    logging.info("Gmail service built successfully.")

    # Email recipient, subject, and MIMEText object with HTML content
    to_email = 'konstantin.kobranov@gmail.com'
    subject = 'Luxembourg Bull Production Data'
    message = MIMEText(email_content, 'html')
    message['to'] = to_email
    message['from'] = 'kobranov.konstantin@gmail.com'
    message['subject'] = subject

    # Encode the MIMEText message in a format accepted by the Gmail API
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    message_body = {'raw': raw}

    # Attempt to send the email using Gmail API
    try:
        message = service.users().messages().send(userId='me', body=message_body).execute()
        logging.info(f'Email sent successfully: Message Id: {message["id"]}')
        return f"Email sent successfully: {message['id']}", 200
    except Exception as error:
        logging.error(f"An error occurred: {error}")
        return f"An error occurred: {error}", 500