# Setup

## Fork and clone the repository
* Fork https://github.com/saini5/cymphony4cs.git and then do git clone to your local destination folder.
* If you have PyCharm, the cloning step can be done via VCS -> Get from Version Control -> Git -> URL: or based on the instructions found [here](https://www.jetbrains.com/help/pycharm/manage-projects-hosted-on-github.html#clone-from-GitHub)
* If you are on an Amazon Linux machine, 
  - `sudo yum -y update`
  - `sudo yum install git`
  - `git clone https://github.com/saini5/cymphony4cs.git`
* Go to the directory cymphony4cs/cymphony4cs (`cd cymphony4cs/cymphony4cs`) and open settings.py (`vim settings.py`) to modify the database settings as per your needs. (see below for more details)

## Setup the database
  - Remarks: Commands given are for setup on Amazon Linux
  - Install postgresql.
    - `sudo yum -y update`
    - `sudo amazon-linux-extras | grep postgre`
    - ```shell
      sudo tee /etc/yum.repos.d/pgdg.repo<<EOF
      > [pgdg12]
      > name=PostgreSQL 12 for RHEL/CentOS 7 - x86_64
      > baseurl=https://download.postgresql.org/pub/repos/yum/12/redhat/rhel-7-x86_64
      > enabled=1
      > gpgcheck=0
      > EOF
      ```
    - `sudo yum makecache`
    - `sudo yum install postgresql12 postgresql12-server`
  - Initialize the database.
    - `sudo /usr/pgsql-12/bin/postgresql-12-setup initdb`
    - `sudo systemctl start postgresql-12`
    - `sudo systemctl status postgresql-12`
  - Create a new database and user.
    - `sudo su - postgres`
    - `psql -c "CREATE DATABASE cymphony4cs;"`
    - `psql -c "CREATE USER cymphony_user WITH PASSWORD 'toor';"`
  - Alter the role of the user and grant privileges to the database.
    - `psql -c "ALTER ROLE cymphony_user SET client_encoding TO 'utf8';"`
    - `psql -c "ALTER ROLE cymphony_user SET default_transaction_isolation TO 'read committed';"`
    - `psql -c "ALTER ROLE cymphony_user SET timezone TO 'UTC';"`
    - `psql -c "GRANT ALL PRIVILEGES ON DATABASE cymphony4cs TO cymphony_user;"`
    - For PostgreSQL v15 and onwards, you also need to run 
      - GRANT ALL ON ALL TABLES IN SCHEMA public to cymphony_user; 
      - GRANT ALL ON ALL SEQUENCES IN SCHEMA public to cymphony_user; 
      - GRANT ALL ON ALL FUNCTIONS IN SCHEMA public to cymphony_user; 
      - GRANT ALL ON SCHEMA public TO cymphony_user;
  - Configure the database. (Commands given for setup on Amazon Linux)
    - `sudo su`
    - `cd pgsql`
    - `cd data`
    - `cp pg_hba.conf ~/pg_hba.conf.backup`
    - `cp postgresql.conf  ~/postgresql.conf.backup`
    - Open pg_hba.conf by `vim pg_hba.conf` and add two lines.
      - host    all     all             0.0.0.1/0            md5
      - host    all     all             ::/0                 md5
    - Open postgresql.conf by `vim postgresql.conf` and modify three lines
      - listen_addresses = '*'          
      - max_connections = 300 
      - shared_buffers = 8GB            
    - `exit`
    - `sudo systemctl restart postgresql-12`
  - Create base tables and functions in the database.
    - Run the script `all_base_tables_with_constraints.sql` to setup all base tables.
    - Run the script `all_21_together.sql` to setup all functions (along with indices)
  - The database is now ready to be used by cymphony.
## Setup the virtual environment
  - Install Anaconda3 by following the instructions [here](https://conda.io/projects/conda/en/latest/user-guide/install/linux.html)
  - Create a virtual environment by running the command `conda create --name cymphony4cs python=3.8`
  - Activate the virtual environment by running the command `conda activate cymphony4cs`
  - To get a list of all environments, run the command `conda info --envs`
  - To get a list of all the packages installed in the current environment, run the command `conda list`
  - (TODO the txt file setup) Install the dependencies via conda by running the command `conda install --file requirements.txt`
  - Install the dependencies inside the conda environment by running the following commands:
    - `conda install boto3=1.17.109`
    - `conda install django=3.2`
    - `conda install xmltodict`
    - `conda install requests=2.27.1`
    - `pip install django-registration==3.2`
    - `pip install psycopg2-binary==2.9.3`
    - `pip install gunicorn==20.1.0`
    - `pip install whitenoise==6.7.0`
      - later versions of whitenoise require python >= 3.9 
## Deploy the system
  - Navigate to the directory `cd cymphony4cs/cymphony4cs` and update settings.py file: `vim settings.py`
    - You will see some settings that are being loaded in from environment variables.
    - You will have to set these environment variables in your system via the command line / shell.
    - For example, you will set the django secret key to a random string
      - SECRET_KEY=your_random_string
    - Another example, you will set the database configuration based on our set up in the previous step.
      - DB_NAME=cymphony4cs 
      - DB_USER=cymphony_user
      - and so on.
    - For long term use, you can set these environment variables in your .bash_profile or .bashrc file.
      - In windows, 
        - you would want to set these environment variables in a batch (.bat) file.
          - @echo off 
          - set DB_NAME=cymphony4cs
          - set DB_USER=cymphony_user
        - and then run the batch file to bring these environment variables into your current command line.
          - `call batch_file_name.bat`

  - Deploy the system by running the below commands. Make sure you are in the directory `cymphony4cs/`, which contains manage.py file.
    - Make sure to set the environment variables before proceeding further.
    - `python manage.py makemigrations`
    - `python manage.py migrate`
    - `python manage.py collectstatic`
    - `python manage.py runserver 0.0.0.0:8000`. (Runserver is a toy server provided by Django and should not be used in production.)
    - For production, use gunicorn server. Run the command `gunicorn --bind 0.0.0.0:8000 --workers 10 --threads 10 cymphony4cs.wsgi`
      - If using from the same machine, open a browser  and navigate to the url http://localhost:8000  
      - If using from a different machine, open a browser and navigate to the url http://<ip-address-of-the-server>:8000
  - You should see the home page of the system. 
    - If you see the home page, you have successfully deployed the system.
    - If you see an error, you have to debug the error. The error could be due to the following reasons:
      - The database is not set up properly, or not running.
      - The database configuration in the settings.py file is incorrect.
      - The static files are not set up properly.
      - The dependencies are not installed properly.
      - The virtual environment is not activated.
      - The port 8000 is not open, or the firewall is blocking the port 8000.
      - The server is not running.
      - The server is not accessible from the client machine due to firewall, network issues, or incorrect IP address.
