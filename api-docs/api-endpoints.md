# Overview
- The API endpoints have been implemented using Django's built-in views. 
- Due to this, they can be accessed via plain HTTP requests which in turn return the appropriate data in response. 
- To access the API using plain HTTP requests, we list the URL of the endpoints, alongwith any required parameters.

**Remarks: The API can be improved by migrating to use Django Rest Framework, which we will try to do some time later.**

# Endpoints

## Endpoint: `/one_step_register/`

### Request Method

`POST`

### Request Parameters
- `first_name` (string): The user's first name.
- `last_name` (string): The user's last name.
- `email` (string): A valid email address.
- `username` (string): The user's username.
- `password1` (string): The user's password.
- `password2` (string): The user's password, should match with `password1`

### Request and Response Formats
- Request format: JSON
- Response format: JSON

### Error Handling
- If any of the required parameters are missing, the API will return a `400` status code and an error message
- If the provided email or username is already in use, the API will return a `401` status code and an error message
- If the provided passwords do not match, the API will return a `401` status code and an error message

### Authentication and Authorization
- This endpoint requires the user to be logged out. Attempting to access it while logged in will return a `401` status code and an error message

### Examples
```python
# Cymphony URL
target_url = 'http://127.0.0.1:8000'

# Create a session object to handle user sessions
s = Session()

# Set up retry mechanism for failed requests
submit_retries = Retry(
    total=5, 
    backoff_factor=1, 
    status_forcelist=[500, 502, 503, 504], 
    method_whitelist=False
)

# Mount the retry mechanism on the session object
s.mount(target_url, HTTPAdapter(max_retries=submit_retries))

# Set the user agent for the request
s.headers = {'User-Agent': 'python-requests/1.2.0'}

# Set the user's credentials
user_name = 'example_requester'
password = 'Th1sIsATe5t'

# Construct the signup URL
signup_url = target_url + '/one_step_register/'

# Set the data to be sent in the request
signup_data = { 
    'first_name': 'first_name_of_requester', 
    'last_name': 'last_name_of_requester', 
    'email': user_name + '@email.com', 
    'username': user_name, 
    'password1': password, 
    'password2': password
}

# Send the POST request and get the response
signup_response = s.post(signup_url, data=signup_data)

# Print the response content
print(signup_response.content)
```

### Additional remarks
- The endpoint /one_step_register/ allows the user to signup to the system in one step.
- The endpoint uses the python requests library to handle the post request. It also uses the python requests library's session object to handle user sessions, retry mechanism for handling retries in case of failed requests, http adapter for handling the retries, and header to specify the user agent.
- The example code uses the Session object to handle user sessions, which is a good practice for security. However, it's important to make sure that user credentials are not hardcoded in the code and that sensitive information such as passwords are stored securely. We used hardcoded password for simplicity and understandability, but please consider using environment variables in your code for handling sensitive information.



## Endpoint: `/controller/?category=project&action=create`

### Request Method

`POST`

### Request Parameters
- `pname` (string): The name of the project to be created.
- `pdesc` (string): The description of the project to be created.

### Request and Response Formats
- Request format: JSON
``` 
{
    "pname": "sample_project",
    "pdesc": "experimental project"
}
```
- Response format: JSON
```
{
    "status": "success",
    "project_id": "123456789"
}
```

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Construct the URL for the endpoint
create_project_url = target_url + '/controller/?category=project&action=create'

# Define the data to be sent in the request
create_project_data = {'pname': 'sample_project', 'pdesc': 'experimental project'}

# Make a POST request to the endpoint with the data
create_project_response = s.post(create_project_url, data=create_project_data)

# Print the response content
print(create_project_response.content)

# Get the project ID from the response and store it in a variable
project_id = create_project_response.json().get('project_id')
```

### Additional remarks
- None.


## Endpoint: `/controller/?category=workflow&action=create&pid=<project_id>`

### Request Method

`POST`

### Request Parameters
- `wname` (string): The name of the workflow to be created.
- `wdesc` (string): The description of the workflow to be created.

### Request and Response Formats
- Request format: JSON
``` 
{
    "wname": "sample_workflow",
    "wdesc": "experimental workflow"
}
```
- Response format: JSON
```
{
    "status": "success",
    "workflow_id": "987654321"
}
```

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Construct the URL for the workflow creation endpoint
create_workflow_url = target_url + '/controller/?category=workflow&action=create&pid=' + str(project_id)

# Generate the name for the workflow
workflow_name = 'sample_workflow'

# Create a dictionary with the data for the workflow
create_workflow_data = {'wname': workflow_name, 'wdesc': 'experimental workflow'}

# Send a POST request to the API with the data for the workflow
create_workflow_response = s.post(create_workflow_url, data=create_workflow_data)

# Print the content of the response from the API
print(create_workflow_response.content)

# Extract the ID of the newly created workflow from the API response
workflow_id = create_workflow_response.json().get('workflow_id')

```

### Additional remarks
- None.



## Endpoint: `/controller/?category=workflow&action=edit_workflow_upload_files&pid=<project_id>&wid=<workflow_id>`

### Request Method

`POST`

### Request Parameters
- `fname` (file): The file to be uploaded.

### Request and Response Formats
- Request format: Multipart/Form-Data
- Response format: JSON

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Define the URL for uploading workflow files
upload_workflow_files_url = target_url + '/controller/?category=workflow&action=edit_workflow_upload_files&pid=' + str(project_id) + '&wid=' + str(workflow_id)

# Open the file "data.csv" for reading in binary mode and send a POST request to the API endpoint with the file attached
with open('./data.csv', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})

# Open the file "instructions.html" for reading in binary mode and send a POST request to the API endpoint with the file attached
with open('./instructions.html', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})

# Open the file "layout.html" for reading in binary mode and send a POST request to the API endpoint with the file attached
with open('./layout.html', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})

# Open the file "workflow.cy" for reading in binary mode and send a POST request to the API endpoint with the file attached
with open('./workflow.cy', 'rb') as f:
    upload_workflow_files_response = s.post(upload_workflow_files_url, files={'fname': f})

```

### Additional remarks
- The file to be uploaded must be in binary format, as specified by the rb mode in the open function.


## Endpoint: `/controller/?category=run&action=create&pid=<project_id>&wid=<workflow_id>`

### Request Method

`POST`

### Request Parameters
- `rname` (string): The name of the run.
- `rdesc` (string): The description of the run.

### Request and Response Formats
- Request format: JSON
- Response format: JSON

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Define the API endpoint URL for creating a run
create_run_url = target_url + '/controller/?category=run&action=create&pid=' + str(project_id) + '&wid=' + str(workflow_id)

# Define the data to be sent to the API endpoint
create_run_data = {'rname': 'run_1', 'rdesc': 'running the workflow'}

# Send a POST request to the API endpoint to create a run
create_run_response = s.post(create_run_url, data=create_run_data)

# Print the response from the API
print(create_run_response.content)

# Extract the run ID from the response
run_id = create_run_response.json().get('run_id')

```

### Additional remarks
- None.


## Endpoint: `/controller/?category=run&action=create&pid=<project_id>&wid=<workflow_id>&rid=<run_id>`

### Request Method

`POST`

### Request Parameters
- `access_key_id` (string): The access key ID for accessing AMT.
- `secret_access_key` (string): The secret access key for accessing AMT.

### Request and Response Formats
- Request format: JSON
- Response format: JSON

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Define the URL for the API endpoint
create_run_url = target_url + '/controller/?category=run&action=create&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(run_id)

# Define the data to be sent with the API request
create_run_data = {'access_key_id': 'xxxxxxxxxxxxxxxx', 'secret_access_key': 'yyyyyyyyyyyyyyyyy'}

# Make the API request
create_run_response = s.post(create_run_url, data=create_run_data)

# Print the response from the API
print(create_run_response.content)

```

### Additional remarks
- This cymphony api endpoint needs to be used if you have a 3a_amt operator in your cy file.
- Cymphony will use the access key ID and secret access key to post your 3a_amt jobs to amt on your behalf.
- That's why you need to already have an amt account with your amt api keys generated. These amt api keys need to be specified here as access key ID and secret access key.


## Endpoint: `/controller/?category=run&action=view&pid=<project_id>&wid=<workflow_id>&rid=<run_id>`

### Request Method

`GET`

### Request Parameters
- None.

### Request and Response Formats
- Request format: None
- Response format: JSON

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Construct the URL
view_run_url = target_url + '/controller/?category=run&action=view&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(run_id)

# Send a GET request to the URL
view_run_data = None
view_run_response = s.get(view_run_url, data=view_run_data)

# Print the response
print(view_run_response.content)

# Extract the list of file names from the response
list_file_names = view_run_response.json().get('list_file_names')

```

### Additional remarks
- None.

## Endpoint: `/controller/?category=run&action=download_file&pid=<project_id>&wid=<workflow_id>&rid=<run_id>&fname=<file_name>`

### Request Method

`GET`

### Request Parameters
- `pid` (int): The ID of the project.
- `wid` (int): The ID of the workflow.
- `rid` (int): The ID of the run.
- `fname` (string): The name of the file to be downloaded.

### Request and Response Formats
- Request format: None
- Response format: Binary

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Construct the URL for the endpoint
download_file_url = target_url + '/controller/?category=run&action=download_file&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(run_id) + '&fname=' + 'final_labels.csv'

# Set the data for the GET request to None
download_file_data = None

# Make the GET request to the endpoint
download_file_response = s.get(download_file_url, data=download_file_data)

# Print the content of the response
print(download_file_response.content)

# Open a file for writing in binary mode
file = open("downloaded_copy.csv", "wb")

# Write the content of the response to the file
file.write(download_file_response.content)

# Close the file
file.close()

```

### Additional remarks
- The response content must be written to a file in binary format, as specified by the wb mode in the open function.



## Endpoint: `/controller/?category=simulated_run&action=create&pid=<project_id>&wid=<workflow_id>`

### Request Method

`POST`

### Request Parameters
- `s_r_name` (string): Name of the simulated run.
- `s_r_desc` (string): Description of the simulated run.

### Request and Response Formats
- Request format: JSON
- Response format: JSON

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Build the URL for the endpoint
create_simulated_run_url = target_url + '/controller/?category=simulated_run&action=create&pid=' + str(project_id) + '&wid=' + str(workflow_id)

# Prepare the data to send with the request
create_simulated_run_data = {'s_r_name': 'simulated_run_1', 's_r_desc': 'first trial'}

# Make the POST request to the endpoint
create_simulated_run_response = s.post(create_simulated_run_url, data=create_simulated_run_data)

# Print the response content
print(create_simulated_run_response.content)

# Extract the simulated run ID from the response
simulated_run_id = create_simulated_run_response.json().get('simulated_run_id')

# Extract the job information from the response
job_info: dict = create_simulated_run_response.json().get('job_info')

# Extract the job IDs from the job information
job_ids = job_info.keys()

# Convert the job IDs to a string and get the first one
job_id = str(list(job_ids)[0])

```

### Additional remarks
- The endpoint returns the ID of the created simulated run and the job information.
- The job information includes the job IDs, status, and other relevant details.


## Endpoint: `/controller/?category=simulated_run&action=create&pid=<project_id>&wid=<workflow_id>&rid=<simulated_run_id>`

### Request Method

`POST`

### Request Parameters
- `min_loop_times_job_<job_id>` (int): The minimum number of times a job should run.
- `max_loop_times_job_<job_id>` (int): Description of the simulated run.
- `min_workers_per_burst_job_<job_id>` (int): The minimum number of workers to work on a job in each burst.
- `max_workers_per_burst_job_<job_id>` (int): The maximum number of workers to work on a job in each burst.
- `min_time_gap_in_loop_points_job_<job_id>` (int): The minimum time gap between loop points in seconds.
- `max_time_gap_in_loop_points_job_<job_id>` (int): The maximum time gap between loop points in seconds.
- `min_worker_annotation_time_job_<job_id>` (int): The minimum time in seconds for a worker to annotate a job.
- `max_worker_annotation_time_job_<job_id>` (int): The maximum time in seconds for a worker to annotate a job.
- `min_worker_accuracy_job_<job_id>` (int): The minimum accuracy for a worker to annotate a job.
- `max_worker_accuracy_job_<job_id>` (int): The maximum accuracy for a worker to annotate a job.

### Request and Response Formats
- Request format: JSON
```
{
    "min_loop_times_job_<job_id>": 1,
    "max_loop_times_job_<job_id>": 1,
    "min_workers_per_burst_job_<job_id>": 1,
    "max_workers_per_burst_job_<job_id>": 1,
    "min_time_gap_in_loop_points_job_<job_id>": 60,
    "max_time_gap_in_loop_points_job_<job_id>": 60,
    "min_worker_annotation_time_job_<job_id>": 1,
    "max_worker_annotation_time_job_<job_id>": 1,
    "min_worker_accuracy_job_<job_id>": 1,
    "max_worker_accuracy_job_<job_id>": 1
}
```
- Response format: JSON
```
{
  "simulated_run_id": 42,
  "job_info": {
    "123": {
      "job_id": 123,
      "job_name": "Job 1",
      ...
    },
    ...
  }
}
```

### Error Handling
- If the specified project id, workflow id, or simulated run id is not found, the server will return a 404 Not Found error.
- If the request data is missing required parameters, the server will return a 400 Bad Request error.
- If the request is unauthorized, the server will return a 401 Unauthorized error.
- If the request is not properly authenticated, the server will return a 403 Forbidden error.

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Create URL for the API endpoint
create_simulated_run_url = target_url + '/controller/?category=simulated_run&action=create&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(simulated_run_id)

# Define the data to be sent with the API request
create_simulated_run_data = {
    'min_loop_times_job_' + job_id: '1', 'max_loop_times_job_' + job_id: '1',
    'min_workers_per_burst_job_' + job_id: '1', 'max_workers_per_burst_job_' + job_id: '1',
    'min_time_gap_in_loop_points_job_' + job_id: '60', 'max_time_gap_in_loop_points_job_' + job_id: '60',
    'min_worker_annotation_time_job_' + job_id: '1', 'max_worker_annotation_time_job_' + job_id: '1',
    'min_worker_accuracy_job_' + job_id: '1', 'max_worker_accuracy_job_' + job_id: '1'
}

# Make the API request
create_simulated_run_response = s.post(create_simulated_run_url, data=create_simulated_run_data)

# Print the response from the API request
print(create_simulated_run_response.content)

```

### Additional remarks
- The pid parameter must correspond to an existing project in the system.
- The wid parameter must correspond to an existing workflow within the specified project.
- The rid parameter must correspond to an existing simulated run within the specified workflow.
- The job_info dictionary keys must correspond to existing job ids within the specified simulated run.

## Endpoint: `/controller/?category=simulated_run&action=view&pid=<project_id>&wid=<workflow_id>&rid=<simulated_run_id>`

### Request Method

`GET`

### Request Parameters
- None.

### Request and Response Formats
- Request format: None
- Response format: JSON

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- To access this endpoint, the client must provide valid authentication credentials.

### Examples
```python

# Form the URL for the endpoint
view_simulated_run_url = (
    target_url
    + "/controller/?category=simulated_run&action=view&pid="
    + str(project_id)
    + "&wid="
    + str(workflow_id)
    + "&rid="
    + str(simulated_run_id)
)

# Set the data for the request
view_simulated_run_data = None

# Make a GET request to the URL with the data
view_simulated_run_response = s.get(view_simulated_run_url, data=view_simulated_run_data)

# Print the content of the response
print(view_simulated_run_response.content)

# Get the list of file names from the response
list_file_names = view_simulated_run_response.json().get("list_file_names")

```

### Additional remarks
- None.


## Endpoint: `/controller/?category=simulated_run&action=download_file&pid=<project_id>&wid=<workflow_id>&rid=<simulated_run_id>&fname=<file_name>`

### Request Method

`GET`

### Request Parameters
- None.

### Request and Response Formats
- Request format: None
- Response format: Binary (representing contents of the requested file)

### Error Handling
- In case of any error, the response will contain a JSON object with the following format:
```
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization
- The authentication details should be passed in the s object, which is a session object in the code.

### Examples
```python

# Define the endpoint URL for downloading a file
download_file_url = target_url + '/controller/?category=simulated_run&action=download_file&pid=' + str(project_id) + '&wid=' + str(workflow_id) + '&rid=' + str(simulated_run_id) + '&fname=' + 'final_labels.csv'

# Set the data for the request as None
download_file_data = None

# Send a GET request to the endpoint
download_file_response = s.get(download_file_url, data=download_file_data)

# Print the content of the response
print(download_file_response.content)

# Open a new file to write the content of the response
file = open("donwloaded_copy.csv", "wb")

# Write the content of the response to the file
file.write(download_file_response.content)

# Close the file
file.close()

```

### Additional remarks
- None.

## Endpoint: `/controller/?category=curation_run&action=create`

### Request Method

`POST`

### Request Parameters

- `workflow_file` (file): The workflow definition file (`.cy`).
- `data_file` (file): The data file (`.csv`) containing items to be curated.
- `id_field_name` (string): The name of the ID field in the `data_file` that uniquely identifies each tuple.
- `instructions_file` (file): The HTML file containing instructions for curators.
- `layout_file` (file): The HTML file defining the layout of the curation interface.
- `notification_url` (string, optional): A URL to which Cymphony will send a POST request upon run completion.

### Request and Response Formats

- Request format: Multipart/Form-Data
- Response format: JSON
```json
{
    "status": "success",
    "message": "Curation run created",
    "run_id": "user_id.project_id.workflow_id.run_id"
}
```

### Error Handling

- In case of any error, the response will contain a JSON object with the following format:
```json
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization

- To access this endpoint, the client must provide valid authentication credentials.

### Examples

```python
# Construct the URL for the curation run creation endpoint
create_curation_run_url = target_url + '/controller/?category=curation_run&action=create'

# Define the paths to the workflow files
workflow_dir = Path("fake-smartcat-exps/bulk-curation/test-workflow")
dict_file_paths = {
    'workflow_file': workflow_dir / "workflow.cy",
    'data_file': workflow_dir / "data.csv",
    'instructions_file': workflow_dir / "instructions.html",
    'layout_file': workflow_dir / "layout.html"
}

# Open the files in binary mode
dict_files = dict()
for key, value in dict_file_paths.items():
    dict_files[key] = open(value, 'rb')

# Define other form data
data_file_id_field_name = "smartcat_id"
notification_url = "http://localhost:5000/webhook" # Replace with actual webhook URL
form_data = {
    'id_field_name': data_file_id_field_name,
    'notification_url': notification_url
}

# Send a POST request to the API with the form data and files
create_curation_run_response = s.post(create_curation_run_url, data=form_data, files=dict_files)

# Close the opened files
for f_obj in dict_files.values():
    f_obj.close()

# Print the response content
print(create_curation_run_response.content)

# Extract the composite run ID from the response
composite_run_id = create_curation_run_response.json().get('run_id')
```

### Additional remarks

- This endpoint is used by external systems like Smartcat to initiate a bulk curation process by providing all necessary workflow and data files, along with an optional notification URL for completion callbacks.


## Endpoint: `/controller/?category=curation_run&action=drive_by_curate`

### Request Method

`POST`

### Request Parameters

- `run_id` (string): The composite run ID (e.g., `user_id.project_id.workflow_id.run_id`) to which these curations belong.
- `curations` (JSON array of arrays): A list of ad-hoc curations, where each curation is `[external_tuple_id, worker_id, annotation]`.

### Request and Response Formats

- Request format: JSON
```json
{
    "run_id": "user_id.project_id.workflow_id.run_id",
    "curations": [
        ["external_id_1", 123, "annotation_A"],
        ["external_id_2", 124, "annotation_B"]
    ]
}
```
- Response format: JSON
```json
{
    "status": "success",
    "message": "Drive by curations received and processed"
}
```

### Error Handling

- In case of any error, the response will contain a JSON object with the following format:
```json
{
    "status": "error",
    "message": "Error message"
}
```

### Authentication and Authorization

- To access this endpoint, the client must provide valid authentication credentials.

### Examples

```python
# Define the API endpoint URL for sending ad-hoc curations
drive_by_curate_url = target_url + '/controller/?category=curation_run&action=drive_by_curate'

# Replace with the actual composite run ID from the initial curation run creation
composite_run_id_for_curation = "user_id.project_id.workflow_id.run_id" # This should be obtained from client.create_curation_run

# Example curations data
curations_data_example = [
    ["item_a", 99999, "Yes"],
    ["item_b", 99996, "No"]
]

# Prepare the data to be sent as JSON
drive_by_data = {
    'run_id': composite_run_id_for_curation,
    'curations': curations_data_example
}

# Send a POST request with JSON data
drive_by_response = s.post(drive_by_curate_url, json=drive_by_data)

# Print the response content
print(drive_by_response.content)
```

### Additional remarks

- This endpoint allows external systems to send ad-hoc annotations for tasks within an existing curation run. These annotations are recorded and trigger aggregation if the tasks are ready.