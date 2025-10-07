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
from django.urls import reverse

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

    def _post(self, endpoint, data, is_json=True):
        url = f"{self.base_url}{endpoint}"
        logger.info(f"POSTing to {url} with data: {data}")
        if is_json:
            response = self.session.post(url, json=data)
        else:
            response = self.session.post(url, data=data) # For form-urlencoded, if any
        response.raise_for_status() # Raise an exception for HTTP errors
        return response.json()

    def _get(self, endpoint, params=None):
        url = f"{self.base_url}{endpoint}"
        logger.info(f"GETting from {url} with params: {params}")
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def login(self, username, password):
        login_url = f"{self.base_url}/login/"
        # Django's LoginView expects form data, not JSON
        self.session.auth(username, password)
        response = self.session.post(login_url, data={'username': username, 'password': password})
        response.raise_for_status()
        # TODO: Check 
        if 'csrftoken' in self.session.cookies:
            self.session.headers.update({'X-CSRFToken': self.session.cookies['csrftoken']})
        logger.info(f"Logged in {username}. CSRF Token: {self.session.headers.get('X-CSRFToken')}")
        login_response_url = response.url
        print('Login response url: ', login_response_url)
        login_page_url = self.base_url + reverse('account:login')
        if login_response_url == login_page_url:
            # login did not go through, so the response landed on the login page again
            print('Request url: ', login_url)
            print('Response resulting url: ', login_response_url)
            print('Login page url: ', login_page_url)
            print('Comparison: ', login_response_url == login_page_url)
            raise Exception("Login failed. Check username/password or Cymphony logs.")
        
        # Alternatively,Check if dashboard was reached
        dashboard_page_url = self.base_url + reverse('account:dashboard')
        print('Dashboard page url: ', dashboard_page_url)
        # if 'dashboard' not in response.url:
        #     raise Exception("Login failed. Check username/password or Cymphony logs.")

    def create_project(self, project_name, project_description):
        endpoint = '/controller/?category=project&action=create'
        data = {'pname': project_name, 'pdesc': project_description}
        response = self._post(endpoint, data)
        return response['project_id']

    def create_workflow(self, project_id, workflow_name, workflow_description):
        endpoint = f'/controller/?category=workflow&action=create&pid={project_id}'
        data = {'wname': workflow_name, 'wdesc': workflow_description}
        response = self._post(endpoint, data)
        return response['workflow_id']

    def upload_workflow_file(self, project_id, workflow_id, file_path):
        endpoint = f'/controller/?category=workflow&action=edit_workflow_upload_files&pid={project_id}&wid={workflow_id}'
        with open(file_path, 'rb') as f:
            files = {'fname': f}
            url = f"{self.base_url}{endpoint}"
            logger.info(f"Uploading file {file_path} to {url}")
            response = self.session.post(url, files=files)
            response.raise_for_status()
            return response.json()

    def create_run(self, project_id, workflow_id, run_name, run_description, notification_url):
        """Create Cymphony based run."""
        endpoint = f'/controller/?category=run&action=create&pid={project_id}&wid={workflow_id}'

        # --- Current simplified approach for Cymphony backend ---
        # Cymphony's /controller/ endpoint currently expects form-urlencoded data.
        # The `notification_url` is passed as a simple POST parameter.
        data_for_cymphony = {
            'rname': run_name,
            'rdesc': run_description,
            'notification_url': notification_url,
        }

        # --- Future/more secure approach (commented out for now) ---
        # callback_info = {
        #     'url': notification_url,
        #     'headers': {'X-Smartcat-Id': 'smartcat-instance-1'}, # Example custom header
        #     'secret': callback_secret # When Cymphony supports webhook secrets
        # }
        # data_with_callback_json = {
        #     'rname': run_name,
        #     'rdesc': run_description,
        #     'callback': callback_info # For a dedicated API endpoint accepting JSON
        # }

        response = self._post(endpoint, data=data_for_cymphony, is_json=False)
        return response['run_id']

    def get_run_status(self, project_id, workflow_id, run_id):
        endpoint = f'/controller/?category=run&action=view&pid={project_id}&wid={workflow_id}&rid={run_id}'
        return self._get(endpoint)

    def download_file(self, project_id, workflow_id, run_id, file_name):
        endpoint = f'/controller/?category=run&action=download_file&pid={project_id}&wid={workflow_id}&rid={run_id}&fname={file_name}'
        url = f"{self.base_url}{endpoint}"
        logger.info(f"Downloading file {file_name} from {url}")
        response = self.session.get(url, stream=True)
        response.raise_for_status()
        return response.content

# --- Fake Smartcat Webhook Receiver ---
app = Flask(__name__)
received_callbacks = [] # Store received callbacks

@app.route('/webhook', methods=['POST'])
def smartcat_webhook_receiver():
    try:
        payload = request.get_json()
        logger.info(f"Received webhook: {json.dumps(payload)}")
        
        # --- Optional: Verify webhook signature (commented out for now) ---
        # signature = request.headers.get('X-Webhook-Signature')
        # if signature and WEBHOOK_SECRET:
        #     import hmac, hashlib
        #     expected_signature = hmac.new(WEBHOOK_SECRET.encode(), request.data, hashlib.sha256).hexdigest()
        #     if not hmac.compare_digest(f'sha256={expected_signature}', signature):
        #         logger.warning("Webhook signature verification failed.")
        #         return jsonify({"status": "error", "message": "Invalid signature"}), 401
        
        received_callbacks.append(payload)
        return jsonify({"status": "received", "message": "Callback processed"}), 200
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def run_flask_app():
    app.run(port=5000, debug=True) # debug=False for production use

# --- Main Simulation Logic ---
def simulate_bulk_curation():
    client = CymphonyClient(CYMPHONY_URL)
    run_id = None
    workflow_dir = None
    try:
        logger.info("--- Starting Fake Smartcat Bulk Curation Simulation ---")

        # 1. SC logs in to CN
        logger.info(f"Logging in as {SMARTCAT_USERNAME}...")
        client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
        logger.info("Login successful.")

        # 2. SC creates a project
        project_name = f"Smartcat_Bulk_Project_{int(time.time())}"
        project_description = "Project for Smartcat bulk curation experiments"
        logger.info(f"Creating project '{project_name}'...")
        project_id = client.create_project(project_name, project_description)
        logger.info(f"Project created with ID: {project_id}")

        # 3. SC sends over workflow and creates a run
        # Create dummy workflow files first
        workflow_dir = Path("temp_workflow_files")
        workflow_dir.mkdir(exist_ok=True)
        (workflow_dir / "workflow.cy").write_text("dummy workflow content") # TODO: Needs to change.
        (workflow_dir / "data.csv").write_text("col1,col2\n1,2\n3,4")
        
        workflow_name = f"Bulk_Workflow_{int(time.time())}"
        workflow_description = "Workflow for bulk curation"
        logger.info(f"Creating workflow '{workflow_name}' for project {project_id}...")
        workflow_id = client.create_workflow(project_id, workflow_name, workflow_description)
        logger.info(f"Workflow created with ID: {workflow_id}")

        logger.info(f"Uploading workflow files to workflow {workflow_id}...")
        client.upload_workflow_file(project_id, workflow_id, workflow_dir / "workflow.cy")
        client.upload_workflow_file(project_id, workflow_id, workflow_dir / "data.csv")
        logger.info("Workflow files uploaded.")

        run_name = f"Bulk_Curation_Run_{int(time.time())}"
        run_description = "Bulk curation run with callback"
        
        # --- IMPORTANT ADAPTATION ---
        # The /controller/ endpoint currently expects form-urlencoded data, not JSON for the 'callback' field.
        # If Cymphony has a dedicated API endpoint that accepts JSON with 'callback' info (as discussed previously, e.g., in api_views.py),
        # this part of the client needs to call THAT endpoint.
        # Alternatively, I can assume the '/controller/?category=run&action=create' endpoint in Cymphony has been updated
        # to handle a JSON body that includes a 'callback' field.
        # For the purpose of this script, I'm simplifying by doing neither, so as to match Cymphony's current POST parameter expectation.
        # When Cymphony's API is updated to accept JSON with a 'callback' object (including secrets),
        # this client's create_run call should be updated to use that.
        logger.info(f"Creating run '{run_name}' with notification URL: {SMARTCAT_WEBHOOK_URL}...")
        run_id = client.create_run(project_id, workflow_id, run_name, run_description, 
                                    SMARTCAT_WEBHOOK_URL)
        logger.info(f"Run created with ID: {run_id}. Awaiting completion notification.")

        # 4. SC calls CN to check status (polling) OR waits for callback
        # For this simulation, we'll implement a polling mechanism with a timeout,
        # but prioritize the callback if received.

        poll_interval = 60 * 5 # seconds (5 minutes)
        max_poll_time = 3600 * 24 # seconds (24 hours)
        start_time = time.time()
        run_completed_via_poll = False
        
        while time.time() - start_time < max_poll_time:
            if received_callbacks:
                logger.info("Callback received! Processing callback and skipping polling.")
                # Process the callback directly
                latest_callback = received_callbacks[-1]
                if latest_callback.get('run_id') == run_id and latest_callback.get('status') == 'COMPLETED':
                    logger.info(f"Run {run_id} completed via webhook notification.")
                    run_completed_via_poll = False # Indicate completion via callback
                    break
            
            logger.info(f"Polling Cymphony for run {run_id} status...")
            status_response = client.get_run_status(project_id, workflow_id, run_id)
            current_status = status_response.get('status') # Assuming 'status' in response
            logger.info(f"Run {run_id} current status: {current_status}")

            if current_status == 'COMPLETED': # Assuming Cymphony returns 'COMPLETED'
                logger.info(f"Run {run_id} completed via polling.")
                run_completed_via_poll = True
                break
            
            time.sleep(poll_interval)
        else:
            logger.warning(f"Run {run_id} did not complete within {max_poll_time} seconds. Timeout.")
            return

        # 5. If run completed (either via poll or callback), download results
        if run_completed_via_poll or received_callbacks:
            logger.info(f"Run {run_id} is complete. Requesting to download final_labels.csv...")# TODO: There needs to be a Cymphony API to download all files.
            try:
                # TODO: Download all files instead.
                # Assuming 'final_labels.csv' is a common output file
                file_content = client.download_file(project_id, workflow_id, run_id, 'final_labels.csv')
                output_filename = f"smartcat_run_{run_id}_final_labels.csv"
                with open(output_filename, 'wb') as f:
                    f.write(file_content)
                logger.info(f"Successfully downloaded results to {output_filename}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download results file: {e}")
        else:
            logger.warning("Run did not complete, skipping file download.")

    except requests.exceptions.HTTPError as e:
        logger.error(f"Cymphony API Error: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        # TODO: Maybe remove this?
        if workflow_dir and workflow_dir.exists():
            shutil.rmtree(workflow_dir)
            logger.info("Cleaned up temporary workflow files.")

if __name__ == "__main__":
    # Start the Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.daemon = True # Allow main program to exit even if thread is running
    flask_thread.start()
    logger.info(f"Fake Smartcat webhook listener started on {SMARTCAT_WEBHOOK_URL}")

    # Give Flask a moment to start
    time.sleep(2) 
    
    # Run the simulation
    simulate_bulk_curation()
    
    logger.info("--- Fake Smartcat Simulation Complete ---")
