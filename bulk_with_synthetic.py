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
"""

from pathlib import Path
import csv

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


from backup_fake_smartcat import CymphonyClient
import time
# --- Configuration ---
CYMPHONY_URL = 'http://127.0.0.1:8000' # Your Cymphony instance URL
SMARTCAT_USERNAME = 'smartcat' # Cymphony user for Smartcat
SMARTCAT_PASSWORD = 'smartcat_password' # Password for Smartcat user

def prepare_run():
    client = CymphonyClient(CYMPHONY_URL)
    client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
    project_name = f"Smartcat_Bulk_Synthetic_Project_{int(time.time())}"
    project_description = "Project for Smartcat bulk curation experiments with synthetic workers"
    project_id = client.create_project(project_name, project_description)
    workflow_name = f"Smartcat_Bulk_Synthetic_Workflow_{int(time.time())}"
    workflow_description = "Workflow for Smartcat bulk curation experiments with synthetic workers"
    workflow_id = client.create_workflow(project_id, workflow_name, workflow_description)
    workflow_dir = Path("fake-smartcat-exps/bulk-curation/synthetic-workers/test-workflow")
    client.upload_workflow_file(project_id, workflow_id, workflow_dir / "workflow.cy")
    client.upload_workflow_file(project_id, workflow_id, workflow_dir / "edi_preprocessed_data.csv")
    client.upload_workflow_file(project_id, workflow_id, workflow_dir / "instructions.html")

    run_name = f"Smartcat_Bulk_Synthetic_Run_{int(time.time())}"
    run_description = "Run for Smartcat bulk curation experiments with synthetic workers"
    run_id, job_info = client.create_simulated_run(project_id, workflow_id, run_name, run_description)
    return project_id, workflow_id, run_id, job_info

def simulate_run(project_id, workflow_id, run_id, job_parameters):
    client = CymphonyClient(CYMPHONY_URL)
    client.login(SMARTCAT_USERNAME, SMARTCAT_PASSWORD)
    response = client.create_simulated_run_with_parameters(project_id, workflow_id, run_id, job_parameters)
    return response


if __name__ == "__main__":
    project_id, workflow_id, run_id, job_info = prepare_run()

    print(f"Run: {run_id}")
    print(f"Job info: {job_info}")
    job_ids = job_info.keys()
    print(f"Job IDs: {job_ids}")
    job_id = str(list(job_ids)[0])
    print(f"Job ID: {job_id}")
    
    job_parameters = {
        'min_loop_times_job_' + job_id: '100', 'max_loop_times_job_' + job_id: '100',
        'min_workers_per_burst_job_' + job_id: '1', 'max_workers_per_burst_job_' + job_id: '1',
        'min_time_gap_in_loop_points_job_' + job_id: '5', 'max_time_gap_in_loop_points_job_' + job_id: '5',
        'min_worker_annotation_time_job_' + job_id: '1', 'max_worker_annotation_time_job_' + job_id: '1',
        'min_worker_accuracy_job_' + job_id: '0.8', 'max_worker_accuracy_job_' + job_id: '0.85'
    }
    print(f"Job parameters: {job_parameters}")

    response = simulate_run(project_id, workflow_id, run_id, job_parameters)
    print(f"Response: {response}")
