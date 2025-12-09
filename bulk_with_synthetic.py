# Simulate a run with synthetic workers
"""
A. Prepare the data.
1. Load in the dataset from the given csv file.
    a. Rename the 'Match' column to 'gold_label' column.
    b. Preprocess it to remove the rows where the 'gold_label' column entry does not exist or is empty.
    c. Store the preprocessed dataset in a new csv file - this is the data file.
2. Create a workflow file with 3a_knlm operator.
3. Create an instructions file.

B. Prepare the run.
1. Log in with lets say the smartcat user account.
2. Create a project.
3. Create a workflow.
4. Upload the workflow file, instructions file, and the data file to the workflow.
5. Create a simulated_run, and get back the run id and job info.

C. Simulate the run.
1. Make another call to create simulated_run but this time specifying the run id and simualtion parameters for each job.
    a. Since there is only one job, the simulation parameters would be:
        min_loop_times: 3
        max_loop_times: 3
        min_workers_per_burst: 1
        max_workers_per_burst: 1
        min_time_gap_in_loop_points: 60
        max_time_gap_in_loop_points: 60
        min_worker_annotation_time: 10
        max_worker_annotation_time: 10
        min_worker_accuracy: 0.8
        max_worker_accuracy: 0.85
    b. Manually inspect the tables in postgres while the run is running to see the progress.
    c. Once the run is completed, inspect the tables again, and the statistics of the run.
    d. Copy paste the relevant files, run logs into an exps folder.

D. Download the results.

E. Inspect the results.
1. Look at the final labels table.
2. Join it with the starting data 
3. Get the accuracy of the labels by comparing with the Match (gold_label) column.
"""

from pathlib import Path
import csv
import pandas as pd
from sklearn.metrics import accuracy_score, confusion_matrix
from flask import Flask, request, jsonify
import threading
from backup_fake_smartcat import CymphonyClient
import time
import logging
import json

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def prepare_data():
    # 1. Prepare the data
    data_file_path = Path("./edi250_maverick.csv")
    data_file_id_field_name = "id"

    rows = []

    # read the csv using dictreader
    with open(data_file_path, "r") as f:
        reader = csv.DictReader(f)
        # remove the rows where the Match (gold_label) entry does not exist
        for row in reader:
            if row["gold_label"] is not None and row["gold_label"] != "":
                rows.append(row)
                print(row)

    print(len(rows))
    # write the rows to a new csv file
    with open(data_file_path.with_suffix(".preprocessed.csv"), "w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)  


# --- Configuration ---
CYMPHONY_URL = 'http://127.0.0.1:8000' # Your Cymphony instance URL
SMARTCAT_USERNAME = 'smartcat' # Cymphony user for Smartcat
SMARTCAT_PASSWORD = 'smartcat_password' # Password for Smartcat user
SMARTCAT_WEBHOOK_URL = 'http://127.0.0.1:5000/webhook' # Fake Smartcat's webhook receiver URL

def prepare_run(client, type='kn', notification_url=None):
    client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
    project_name = f"Smartcat_Bulk_Synthetic_Project_{int(time.time())}"
    project_description = "Project for Smartcat bulk curation experiments with synthetic workers"
    user_id, project_id = client.create_project(project_name, project_description)
    workflow_name = f"Smartcat_Bulk_Synthetic_Workflow_{int(time.time())}"
    workflow_description = "Workflow for Smartcat bulk curation experiments with synthetic workers"
    workflow_id = client.create_workflow(project_id, workflow_name, workflow_description)
    workflow_dir = Path("fake-smartcat-exps/bulk-curation/synthetic-workers/test-workflow")
    if type == 'knlm':
        client.upload_workflow_file(project_id, workflow_id, workflow_dir / "workflow_knlm.cy")
    else:
        client.upload_workflow_file(project_id, workflow_id, workflow_dir / "workflow.cy")
    client.upload_workflow_file(project_id, workflow_id, workflow_dir / "edi_preprocessed_data.csv")
    client.upload_workflow_file(project_id, workflow_id, workflow_dir / "instructions.html")

    run_name = f"Smartcat_Bulk_Synthetic_Run_{int(time.time())}"
    run_description = "Run for Smartcat bulk curation experiments with synthetic workers"
    run_id, job_info = client.create_simulated_run(project_id, workflow_id, run_name, run_description, notification_url)
    return user_id, project_id, workflow_id, run_id, job_info


def download_results(client, project_id, workflow_id, run_id, exp_dir):
    response = client.get_simulated_run_status(project_id, workflow_id, run_id)
    print(f"Response: {response}")
    list_file_names = response.get('list_file_names')
    if list_file_names:
        for file_name in list_file_names:
            file_content = client.download_file(project_id, workflow_id, run_id, file_name)
            with open(exp_dir / file_name, 'wb') as f:
                f.write(file_content)
            print(f"Downloaded {file_name} to {exp_dir / file_name}")
    else:
        print("No file names received to download.")

def inspect_results(exp_dir):
    final_labels_file_path = exp_dir / "final_labels.csv"
    final_labels_df = pd.read_csv(final_labels_file_path)
    print(final_labels_df.head())
    
    original_data_file_path = exp_dir / "ORIGINAL_DATA"
    original_data_df = pd.read_csv(original_data_file_path)
    print(original_data_df.head())

    # original_data_df = original_data_df.head(70)
    # final_labels_df = final_labels_df.head(70)
    # print(original_data_df)
    # print(final_labels_df)

    # join the final_labels_df with the original_data_df on the _id column
    final_labels_df = final_labels_df.merge(original_data_df, on='_id', how='inner')
    print(final_labels_df.head())
    # print(final_labels_df)
    print(final_labels_df.shape)
    # get the accuracy of the labels by comparing with the Match (gold_label) column
    numerator = sum(final_labels_df['gold_label'] == final_labels_df['label'])
    denominator = len(final_labels_df)
    accuracy = numerator / denominator
    print(f'Accuracy = {numerator} / {denominator} = {accuracy}')
    accuracy = accuracy_score(final_labels_df['gold_label'], final_labels_df['label'])
    print('Accuracy: ', accuracy)
    # precision = precision_score(final_labels_df['gold_label'], final_labels_df['label'])
    # print('Precision: ', precision)
    # recall = recall_score(final_labels_df['gold_label'], final_labels_df['label'])
    # print('Recall: ', recall)
    # f1 = f1_score(final_labels_df['gold_label'], final_labels_df['label'])
    # print('F1 Score: ', f1)
    cm = confusion_matrix(final_labels_df['gold_label'], final_labels_df['label'], labels=[0.0, 1.0])
    print('Confusion Matrix: ', cm)

    TN, FP, FN, TP = cm.ravel()
    print('TN: ', TN)
    print('FP: ', FP)
    print('FN: ', FN)
    print('TP: ', TP)

    # show those rows where final_labels_df gold_label is not equal to label
    failures = final_labels_df[final_labels_df['gold_label'] != final_labels_df['label']]
    print('All failures: ', failures.shape)
    print(failures.head())
    print(failures)
    failures_fp = failures[failures['gold_label'] == 0.0]
    print('FP failures: ', failures_fp.shape)
    print(failures_fp.head())
    failures_fn = failures[failures['gold_label'] == 1.0]
    print('FN failures: ', failures_fn.shape)
    print(failures_fn.head())



# def get_statistics(project_id, workflow_id, run_id):
#     client = CymphonyClient(CYMPHONY_URL)
#     client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
#     response = client.get_simulated_run_statistics(project_id, workflow_id, run_id)
#     print(f"Response: {response}")
#     return response


# --- Fake Smartcat Webhook Receiver ---
app = Flask(__name__)
received_callbacks = [] # Store received callbacks
callback_received_event = threading.Event() # Event to signal main thread

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
        callback_received_event.set() # Signal that a callback was received
        return jsonify({"status": "received", "message": "Callback processed"}), 200
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def run_flask_app():
    # When running Flask in a separate thread, debug=True and use_reloader=True are problematic
    # Disable debug and reloader for threaded execution
    app.run(port=5000, debug=False, use_reloader=False)

if __name__ == "__main__":
    # Start the Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.daemon = True # Allow main program to exit even if thread is running
    flask_thread.start()
    logger.info(f"Fake Smartcat webhook listener started on {SMARTCAT_WEBHOOK_URL}")

    # Give Flask a moment to start
    time.sleep(2) 

    e2e_start_time = time.time()
    client = CymphonyClient(CYMPHONY_URL)
    # 1. Prepare run.
    user_id, project_id, workflow_id, run_id, job_info = prepare_run(client, type='knlm', notification_url=SMARTCAT_WEBHOOK_URL)

    print(f"Run: {run_id}")
    print(f"Job info: {job_info}")
    job_ids = job_info.keys()
    print(f"Job IDs: {job_ids}")
    job_id = str(list(job_ids)[0])
    print(f"Job ID: {job_id}")
    
    job_parameters = {
        'min_loop_times_job_' + job_id: '100', 'max_loop_times_job_' + job_id: '100',
        'min_workers_per_burst_job_' + job_id: '1', 'max_workers_per_burst_job_' + job_id: '1',
        'min_time_gap_in_loop_points_job_' + job_id: '120', 'max_time_gap_in_loop_points_job_' + job_id: '120',
        'min_worker_annotation_time_job_' + job_id: '10', 'max_worker_annotation_time_job_' + job_id: '10',
        'min_worker_accuracy_job_' + job_id: '0.8', 'max_worker_accuracy_job_' + job_id: '0.85'
    }
    print(f"Job parameters: {job_parameters}")

    time.sleep(2)

    # 2. Simulate the run.
    response = client.create_simulated_run_with_parameters(project_id, workflow_id, run_id, job_parameters)
    print(f"Response: {response}")

    time.sleep(2)

    # 3. Check the status of the run.
    status_response = client.get_simulated_run_status(project_id, workflow_id, run_id)
    print(f"Status response: {status_response}")

    time.sleep(2)

    # Simulate stewards via simulate_stewards.py
    workflow_dir = Path("fake-smartcat-exps/bulk-curation/synthetic-workers/test-workflow")
    data_file_path = workflow_dir / "edi_preprocessed_data.csv"
    size_data_job = len(pd.read_csv(data_file_path))
    print('Size of data pushed in job: ', size_data_job)
    print('Requester ID: ', user_id)

    # statistics = get_statistics(project_id, workflow_id, run_id)  # TODO: Cymphony side needs to be changed to respond with json response in case of api requests
    # print(f"Statistics: {statistics.get('statistics')}")


    # 4. SC calls CN to check status (polling) OR waits for callback
    # For this simulation, we'll implement a polling mechanism with a timeout,
    # but prioritize the callback if received.

    poll_interval = 60 * 5 # seconds (5 minutes)
    max_poll_time = 3600 * 24 # seconds (24 hours)
    start_time = time.time()
    run_completed_via_poll = False
    list_file_names = None
    composite_run_id = f"12.{project_id}.{workflow_id}.{run_id}"
    
    while time.time() - start_time < max_poll_time:
        # Wait for either a callback or the poll interval to pass
        if callback_received_event.wait(timeout=poll_interval):
            logger.info("Callback received! Processing callback and skipping polling.")
            callback_received_event.clear() # Reset the event for the next potential callback
            # Process the callback directly
            latest_callback = received_callbacks[-1] # Process latest callback
            if latest_callback.get('run_id') == composite_run_id and latest_callback.get('status') == 'COMPLETED':
                logger.info(f"Run {run_id} completed via webhook notification.")
                run_completed_via_poll = False # Indicate completion via callback
                list_file_names = latest_callback.get('list_file_names')
                break
            else:
                logger.info("Received a callback, but it's not for completion of this run. Continuing...")

        else: # Timeout occurred, no callback received, proceed with polling
            logger.info(f"Polling Cymphony for run {run_id} status...")
            status_response = client.get_simulated_run_status(project_id, workflow_id, run_id)
            current_status = status_response.get('status') # Assuming 'status' in response
            logger.info(f"Run {run_id} current status: {current_status}")
            list_file_names = status_response.get('list_file_names') # Assuming list of files in response

            if current_status == 'COMPLETED': # Assuming Cymphony returns 'COMPLETED'
                logger.info(f"Run {run_id} completed via polling.")
                run_completed_via_poll = True
                break
        
    else:
        logger.warning(f"Run {run_id} did not complete within {max_poll_time} seconds. Timeout.")
        exit(1)

    # 5. If run completed (either via poll or callback), download results
    exp_dir = Path(f"fake-smartcat-exps/bulk-curation/synthetic-workers/test-workflow/exp_12_{project_id}_{workflow_id}_{run_id}")
    exp_dir.mkdir(parents=True, exist_ok=True)
    if run_completed_via_poll or (received_callbacks and any(cb.get('run_id') == composite_run_id and cb.get('status') == 'COMPLETED' for cb in received_callbacks)):
        logger.info(f"Run {run_id} is complete. Requesting to download files...")
        download_results(client, project_id, workflow_id, run_id, exp_dir)
    else:
        logger.warning("Run did not complete, skipping file download.")

    e2e_end_time = time.time()
    logger.info(f"Total time taken: {int(e2e_end_time - e2e_start_time)} seconds")

    inspect_results(exp_dir)
