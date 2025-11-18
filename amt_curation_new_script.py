import pandas as pd
import numpy as np
import time
import os
from requests import Request, Session
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pathlib import Path

EXP_DIR = Path('./fake-smartcat-exps/amt-curation/real-turkers/dummy_exp/')
TARGET_URL = 'http://127.0.0.1:8000' # Your Cymphony instance URL
SMARTCAT_USERNAME = 'smartcat' # Cymphony user for Smartcat
SMARTCAT_PASSWORD = 'smartcat_password' # Password for Smartcat user
ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

def login(session, username, password):
    login_url = f"{TARGET_URL}/login/"
        
    # Step 1: GET the login page to obtain the CSRF token
    print(f"GEtting login page to retrieve CSRF token from {login_url}")
    get_response = session.get(login_url)
    get_response.raise_for_status()
    
    # Extract CSRF token from cookies
    csrftoken = session.cookies.get('csrftoken')
    if not csrftoken:
        raise Exception("CSRF token not found in login page cookies. Login page might have changed.")
    session.headers.update({'X-CSRFToken': csrftoken})
    
    # Step 2: POST login credentials with the obtained CSRF token
    print(f"POSTing login credentials to {login_url}")
    response = session.post(
        login_url, 
        data={
            'username': username, 
            'password': password,
            'csrfmiddlewaretoken': csrftoken # Include the CSRF token in POST data
        }
    )
    response.raise_for_status()

    # Check if login was successful by examining the redirect URL
    # If redirected back to login URL, it's a failure.
    if response.url == login_url:
        print(f"Login failed. Response URL: {response.url}")
        raise Exception("Login failed. Check username/password or Cymphony logs.")

    print(f"Logged in {username}. CSRF Token: {session.headers.get('X-CSRFToken')}")


def download_file(session, project_id, workflow_id, run_id, file_name):
    endpoint = f'/controller/?category=run&action=download_file&pid={project_id}&wid={workflow_id}&rid={run_id}&fname={file_name}'
    url = f"{TARGET_URL}{endpoint}"
    print(f"Downloading file {file_name} from {url}")
    response = session.get(url, stream=True)
    response.raise_for_status()
    return response.content


# set up cymphony session
s = Session()
submit_retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
s.mount(TARGET_URL, HTTPAdapter(max_retries=submit_retries))
s.headers = {'User-Agent': 'python-requests/1.2.0'}

e2e_start_time = time.time()
print(f"E2E start time: {e2e_start_time}")
# print the current time
print(f"Current time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
login(s, SMARTCAT_USERNAME, SMARTCAT_PASSWORD)

create_project_url = TARGET_URL + '/controller/?category=project&action=create'
create_project_data = {'pname': 'project_for_testing_edi_on_amt', 'pdesc': 'experimental project'}
create_project_response = s.post(create_project_url, data=create_project_data)
print(create_project_response.content)
project_id = create_project_response.json().get('project_id')

# assuming, workflow.cy, layout.html, instructions.html are already prepared, publish data to cymphony
create_workflow_url = TARGET_URL + '/controller/?category=workflow&action=create&pid=' + str(project_id)
workflow_name = 'workflow_for_testing_edi_on_amt'
create_workflow_data = {'wname': workflow_name, 'wdesc': 'experimental workflow'}
create_workflow_response = s.post(create_workflow_url, data=create_workflow_data)
print(create_workflow_response.content)
workflow_id = create_workflow_response.json().get('workflow_id')
upload_workflow_files_url = TARGET_URL + '/controller/?category=workflow&action=edit_workflow_upload_files&pid=' + str(project_id) + '&wid=' + str(workflow_id)
with open(EXP_DIR / 'data' / 'edi250_maverick_preprocessed_1.csv', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})
with open(EXP_DIR / 'instructions' / 'instructions.html', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})
with open(EXP_DIR / 'layout' / 'layout.html', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})
with open(EXP_DIR / 'workflow' / 'workflow.cy', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})


create_run_url = TARGET_URL + '/controller/?category=run&action=create&pid=' + str(project_id) + '&wid=' + str(workflow_id)
create_run_data = {'rname': 'run_for_testing_edi_on_amt', 'rdesc': 'running the workflow'}
create_run_response = s.post(create_run_url, data=create_run_data)
print(create_run_response.content)
run_id = create_run_response.json().get('run_id')

create_run_url = TARGET_URL + '/controller/?category=run&action=create&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(run_id)
create_run_data = {'access_key_id': ACCESS_KEY_ID, 'secret_access_key': SECRET_ACCESS_KEY}
create_run_response = s.post(create_run_url, data=create_run_data)
print(create_run_response.content)


# retrieve data from cymphony
view_run_url = TARGET_URL + '/controller/?category=run&action=view&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(run_id)
view_run_data = None
while (True):
    view_run_response = s.get(view_run_url, data=view_run_data)
    print(view_run_response.content)
    view_run_response_json = view_run_response.json()
    run_status = view_run_response_json.get('status')
    if run_status == 'COMPLETED':
        list_file_names: list = view_run_response_json.get('list_file_names')
        if not list_file_names:
            print("No file names received to download.")
        else:
            for file_name in list_file_names:
                file_content = download_file(s, project_id, workflow_id, run_id, file_name)
                with open(EXP_DIR / 'results' / file_name, 'wb') as f:
                    f.write(file_content)
                print(f"Downloaded {file_name} to {EXP_DIR / 'results' / file_name}")
        break # since the run is completed, we can break the loop
    else:
        print(f"Run {run_id} is not completed. Status: {run_status}")
        time.sleep(30)  # 30 seconds pause in checking results

e2e_end_time = time.time()
print(f"E2E start time: {e2e_start_time}")
print(f"E2E end time: {e2e_end_time}")
print(f"Total time taken: {e2e_end_time - e2e_start_time} seconds")
# analyze results

