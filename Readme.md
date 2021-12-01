  # Before you begin
  This file contains several service definitions:
  - airflow-scheduler - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
  - airflow-webserver - The webserver is available at http://localhost:8080.
  - airflow-worker - The worker that executes the tasks given by the scheduler.
  - airflow-init - The initialization service.
  - flower - The flower app for monitoring the environment. It is available at http://localhost:5555.
  - postgres - The database.
  - redis - The redis - broker that forwards messages from scheduler to worker. 
  
  ### Deploy Airflow on Docker Compose
  1. To get started, download the app
  ```bash
  git clone https://github.com/SemenChepak/Pandas-Airflow.git
  ```
  2. Go to the created folder
  ```bash
  cd Docker-Airflow
  ```
  3. Initialize the database
  ```bash
  docker-compose up airflow-init
  ```
  After initialization is complete, you should see a message like below.
  ```bash
  airflow-init_1       | Upgrades done
  airflow-init_1       | Admin user airflow created
  airflow-init_1       | 2.2.2
  start_airflow-init_1 exited with code 0
 ```
 4. Start Images
  ```bash
  docker-compose up
  ```
  ### Check yor app
  - airflow-webserver -  http://localhost:8080.
  

#Project structure
  ```
├───dags
│   └───SQLalchemy_task
│       ├───Classes
│       │   └───__pycache__
│       ├───db_worker
│       │   └───__pycache__
│       ├───E_B
│       │   ├───cred
│       │   └───__pycache__
│       ├───logs
│       ├───Spark_op
│       │   ├───driver
│       │   └───output
│       │       ├───max_time
│       │       └───parquet
│       │           ├───person_cards
│       │           └───transactions
│       ├───workers
│       │   └───__pycache__
│       └───__pycache__
├───logs
│   ├───Alchemy_persons_and_cards
│   │   ├───check_db_exist
│   │   ├───insert_cards
│   │   ├───insert_person
│   │   └───recheck_db_exist2
│   ├───Alchemy_transactions
│   │   └───insert_tr
│   ├───dag_processor_manager
│   └───scheduler
└───plugins
```
