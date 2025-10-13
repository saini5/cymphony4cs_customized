# Environment installed via conda create -n dummy-smartcat python=3.12.3 flask=3.1.2 requests=2.32.5 urllib3=2.5.0
# Create Smartcat User in Cymphony via python manage.py manage_roles_and_users --create_user "smartcat" "smartcat_password"

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify
import threading
import time
import json
import logging
from pathlib import Path
import shutil
import random
import csv

# --- Configuration ---
CYMPHONY_URL = 'http://127.0.0.1:8000' # Your Cymphony instance URL
SMARTCAT_WEBHOOK_URL = 'http://127.0.0.1:5000/webhook' # Fake Smartcat's webhook receiver URL
SMARTCAT_USERNAME = 'smartcat' # Cymphony user for Smartcat
SMARTCAT_PASSWORD = 'smartcat_password' # Password for Smartcat user
# WEBHOOK_SECRET = 'mys_webhook_secret' # Shared secret for webhook signature verification

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cymphony API Client ---
class CymphonyClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.session = requests.Session()
        # Set up retry mechanism
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers.update({'User-Agent': 'FakeSmartcat-python-requests/1.0'}) # TODO: Cymphony currently returns json response only if 'python' in user-agent. How can I change it so that it returns json response whenever the request is a web api request and not from the browser.

    def _post(self, endpoint, data=None, files=None, json=None):
        if json is not None and (data or files):
            raise ValueError("Pass either json=... or (data=..., files=...), not both.")
        
        url = f"{self.base_url}{endpoint}"
        if json is not None:
            logger.info(f"POSTing to {url} with json: {json}")
            response = self.session.post(url, json=json)
        else:
            logger.info(f"POSTing to {url} with data: {data}")
            response = self.session.post(url, data=data, files=files) # For form-urlencoded, if any
        response.raise_for_status() # Raise an exception for HTTP errors
        # Can sometimes receive a zip, so branch on content-type
        if response.headers.get('Content-Type') == 'application/zip':
            return response.content
        else:
            return response.json()

    def _get(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        logger.info(f"GETting from {url} with params: {params}")
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def login(self, username, password):
        login_url = f"{self.base_url}/login/"
        
        # Step 1: GET the login page to obtain the CSRF token
        logger.info(f"GETting login page to retrieve CSRF token from {login_url}")
        get_response = self.session.get(login_url)
        get_response.raise_for_status()
        
        # Extract CSRF token from cookies
        csrftoken = self.session.cookies.get('csrftoken')
        if not csrftoken:
            raise Exception("CSRF token not found in login page cookies. Login page might have changed.")
        self.session.headers.update({'X-CSRFToken': csrftoken})
        
        # Step 2: POST login credentials with the obtained CSRF token
        logger.info(f"POSTing login credentials to {login_url}")
        response = self.session.post(
            login_url, 
            data={
                'username': username, 
                'password': password,
                'csrfmiddlewaretoken': csrftoken # Include the CSRF token in POST data
            },
            # auth=(username, password) # Basic Auth is typically for stateless APIs, not form logins
                                        # Keep it commented for now as LoginView primarily relies on session + CSRF
        )
        response.raise_for_status()

        # Check if login was successful by examining the redirect URL
        # If redirected back to login URL, it's a failure.
        if response.url == login_url:
            logger.error(f"Login failed. Response URL: {response.url}")
            raise Exception("Login failed. Check username/password or Cymphony logs.")

        logger.info(f"Logged in {username}. CSRF Token: {self.session.headers.get('X-CSRFToken')}")

    def create_curation_run(self, dict_file_paths, data_file_id_field_name, notification_url):
        """Create Cymphony based curation run."""

        # The receiver expects the below
        # workflow_file = request.FILES.get('workflow_file')
        # data_file = request.FILES.get('data_file')
        # id_field_name = request.POST.get('id_field_name', None)
        # instructions_file = request.FILES.get('instructions_file')
        # layout_file = request.FILES.get('layout_file')
        # notification_url = request.POST.get('notification_url', None)

        endpoint = f'/controller/?category=curation_run&action=create'
        dict_files = dict()
        for key, value in dict_file_paths.items():
            dict_files[key] = open(value, 'rb')
        form_data = {
            'id_field_name': data_file_id_field_name,
            'notification_url': notification_url
        }
        response = self._post(endpoint, data=form_data, files=dict_files)
        print("Received the response from Cymphony: ", response)
        return response['run_id']

    def send_ad_hoc_curations(self, run_id, curations):
        endpoint = f'/controller/?category=curation_run&action=drive_by_curate'
        data = {
            'run_id': run_id,
            'curations': json.dumps(curations), # List of [external_tuple_id, annotator_worker_id, annotation]
        }
        logger.info(f"Sending {len(curations)} ad-hoc curations to Cymphony for run {run_id}")
        response = self._post(endpoint, data=data)
        print("Received the response from Cymphony: ", response)
        return response
    
    def get_run_status(self, run_id):
        endpoint = f'/controller/?category=curation_run&action=status'
        logger.info(f"Getting status of run {run_id} from Cymphony")
        response = self._get(endpoint, params={'run_id': run_id})
        logger.info(f"Received the response from Cymphony: {response}")
        return response

    def download_tables(self, run_id, table_names):
        endpoint = f'/controller/?category=curation_run&action=download_tables'
        logger.info(f"Downloading tables {table_names} from Cymphony for run {run_id}")
        contents = self._post(endpoint, data={'run_id': run_id, 'table_names': json.dumps(table_names)})
        logger.info(f"Received the zip contents from Cymphony")
        return contents

def generate_random_curations(data_csv_path: Path, id_field_name: str, num_curations: int):
    """Generates random annotations for tuples from a CSV file. No Pandas required."""
    if not data_csv_path.exists():
        raise FileNotFoundError(f"Data CSV file not found at {data_csv_path}")
    
    with open(data_csv_path, 'r') as f:
        reader = csv.DictReader(f)
        # Store all rows (dictionaries) in a list
        all_rows = list(reader) # Implicitly skips the header row
        curations = []
        # Check if there are any rows to pick from
        if all_rows and len(all_rows) >= num_curations:
            for _ in range(num_curations):
                # Randomly select a random row
                random_dict_row = random.choice(all_rows)
                # Extract the value of the id_field_name
                id = random_dict_row[id_field_name]
                worker_id = random.choice([x for x in range(1, 100000)])
                annotation = random.choice(['Yes', 'No', 'Cannot Determine'])
                curations.append([id, worker_id, annotation])
        else:
            raise ValueError(f"No rows found in the data CSV file at {data_csv_path}")
    return curations

# --- Main Simulation Logic ---
def simulate_drive_by_curation():
    client = CymphonyClient(CYMPHONY_URL)
    run_id = None
    try:
        logger.info("--- Starting Fake Smartcat Drive-by Curation Simulation ---")

        # 1. SC logs in to CN
        logger.info(f"Logging in as {SMARTCAT_USERNAME}...")
        client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
        logger.info("Login successful.")

        time.sleep(2)

        # 2. SC creates a curation run
        workflow_dir = Path("fake-smartcat-exps/drive-by-curation/test-workflow")
        dict_file_paths = {
            'workflow_file': workflow_dir / "workflow.cy",
            'data_file': workflow_dir / "data.csv",
            'instructions_file': workflow_dir / "instructions.html"
        }
        data_file_id_field_name = "id"
        notification_url = SMARTCAT_WEBHOOK_URL
        run_id = client.create_curation_run(dict_file_paths, data_file_id_field_name, notification_url)
        logger.info(f"Curation run created with ID: {run_id}")

        time.sleep(30)

        # 3. SC sends ad-hoc curations

        # Setting up the curations
        num_curations_to_send = 2
        logger.info(f"Starting drive-by curation: sending {num_curations_to_send} ad-hoc annotations...")
        curations_data = generate_random_curations(
            dict_file_paths['data_file'], 
            data_file_id_field_name, 
            num_curations_to_send
        )
        drive_by_response = client.send_ad_hoc_curations(run_id, curations_data)
        if drive_by_response['status'] == 'success':
            logger.info(f"Ad-hoc curations sent to Cymphony for run {run_id}")
        else:
            logger.error(f"Failed to send ad-hoc curations to Cymphony for run {run_id}")
        
        time.sleep(30)

        # 4. SC gets the status of the run
        status_response = client.get_run_status(run_id)
        logger.info(f"Run {run_id} status: {status_response}")

        time.sleep(30)

        # 5. SC downloads the tables
        table_names = ['Assignments', 'Annotations', 'Aggregations']
        download_tables_contents = client.download_tables(run_id, table_names)
        # save the zip contents
        download_tables_dir = Path("fake-smartcat-exps/drive-by-curation/test-workflow")
        zip_file_path = download_tables_dir / f"tables_{run_id}_{int(time.time())}.zip"
        with open(zip_file_path, 'wb') as f:
            f.write(download_tables_contents)
        logger.info(f"Tables downloaded from Cymphony for run {run_id} and placed in {zip_file_path}")
        

    except requests.exceptions.HTTPError as e:
        logger.error(f"Cymphony API Error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        pass


if __name__ == "__main__":

    
    # Run the simulation
    simulate_drive_by_curation()
    
    logger.info("--- Fake Smartcat Simulation Complete ---")
