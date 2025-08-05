# Canvas Data Extraction with Prefect

This project runs a Prefect flow to extract data from a Canvas LMS instance and store it in a PostgreSQL database. The flow is designed to be run on a Prefect server or Prefect Cloud, allowing for easy scheduling and monitoring.

## Requirements

- Python 3.11
- PostgreSQL or MySQL with access to Moodle schema
- Prefect 3.x

## How to Run

### Install dependencies
```bash
pip install -r requirements.txt
```

### Start Prefect API Server (locally)
```
prefect server start
```

This launches:

- Prefect UI: http://127.0.0.1:4200

- Prefect API: http://127.0.0.1:4200/api

### Start a Prefect Worker (new terminal)

```bash
prefect worker start --pool 'canvas-pool'
```

### Deploy the Flow

```
prefect deploy canvas/flow.py:canvas_data_extraction_flow -n "canvas-data-extraction"
```


## Deploy to Prefect Cloud

### Login to Prefect Cloud

```bash
prefect cloud login
prefect profile use cloud-profile
```

### Create a prefect:managed work pool
```bash
prefect work-pool create canvas-pool --type prefect:managed
```

### Deploy the flow
```
python canvas_flow.py
```

### View the Flow Run
- Go to the Prefect Cloud UI: https://cloud.prefect.io/