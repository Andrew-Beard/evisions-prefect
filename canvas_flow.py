import json
import os
from pathlib import Path
from typing import Dict, Any, List
import psycopg2
import requests
from psycopg2.extras import RealDictCursor
import sqlalchemy
from sqlalchemy import create_engine, text
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

from canvas import endpoints
from canvas import APIClient

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

CONNECTION_INFO = {
    "dataSource": {
        "parameters": {
            "hostName": "https://evisions.instructure.com/",
            "accountId": os.getenv("CANVAS_ACCOUNT_ID", "1")
        }
    },
    "credentials": {
        "bearerToken": {
            "secrets": {
                "bearerToken": {
                    "value": os.getenv("CANVAS_API_TOKEN")
                }
            }
        }
    }
}

# Database configuration
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "canvas_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "password")
}


def get_db_connection_string():
    """Get PostgreSQL connection string"""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"


def normalize_json_data(data):
    """
    Normalize JSON data to handle nested objects and arrays
    Convert dict/list values to JSON strings
    """
    if isinstance(data, list):
        return [normalize_json_data(item) for item in data]
    elif isinstance(data, dict):
        normalized = {}
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                # Convert nested objects/arrays to JSON strings
                normalized[key] = json.dumps(value, default=str)
            elif pd.isna(value) or value is None:
                normalized[key] = None
            else:
                normalized[key] = value
        return normalized
    else:
        return data


def prepare_dataframe_for_db(df):
    """
    Prepare DataFrame for database insertion by handling complex data types
    """
    # Create a copy to avoid modifying original data
    df_copy = df.copy()

    for column in df_copy.columns:
        # Check if column contains dict or list objects
        if df_copy[column].dtype == 'object':
            # Convert complex objects to JSON strings
            df_copy[column] = df_copy[column].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, (dict, list)) else x
            )

    return df_copy


@task(name="log-json-results", description="Log JSON results with sample records")
def log_json_results(data: Any, endpoint_name: str, sample_count: int = 1) -> None:
    """Log JSON results with specified number of sample records"""
    logger = get_run_logger()

    if isinstance(data, list) and len(data) > 0:
        logger.info(f"=== {endpoint_name} Sample Results ===")
        logger.info(f"Total records: {len(data)}")

        # Log sample records
        sample_size = min(sample_count, len(data))
        for i in range(sample_size):
            logger.info(f"Sample record {i + 1}:")
            logger.info(json.dumps(data[i], indent=2, default=str))

        if len(data) > sample_count:
            logger.info(f"... and {len(data) - sample_count} more records")
    elif isinstance(data, dict):
        logger.info(f"=== {endpoint_name} Result ===")
        logger.info(json.dumps(data, indent=2, default=str))
    else:
        logger.info(f"=== {endpoint_name} Result ===")
        logger.info(f"Data type: {type(data)}, Value: {data}")


@task(name="test-database-connection", description="Test PostgreSQL database connection")
def test_database_connection() -> bool:
    """Test database connection before starting data extraction"""
    logger = get_run_logger()
    try:
        engine = create_engine(get_db_connection_string())
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise


@task(name="initialize-canvas-api-client", description="Initialize Canvas API client connection")
def initialize_api_client(connection_info: Dict[str, Any]) -> APIClient:
    logger = get_run_logger()
    hostname = connection_info["dataSource"]["parameters"]["hostName"]
    account_id = connection_info["dataSource"]["parameters"]["accountId"]
    logger.info(f"Initializing Canvas API client for {hostname} (Account: {account_id})")
    api_client = APIClient(connection_info)
    logger.info("Canvas API client initialized successfully")
    return api_client


@task(name="fetch-canvas-users", description="Fetch all users from Canvas", retries=2, retry_delay_seconds=30)
def fetch_users_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching users data from Canvas API")
    try:
        data = endpoints.get_users(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} users")

        # Log sample results
        log_json_results(data, "Users")

        return {"endpoint_name": "Users", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch users: {str(e)}")
        raise


@task(name="fetch-canvas-quizzes", description="Fetch all quizzes from Canvas", retries=2, retry_delay_seconds=30)
def fetch_quizzes_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching quizzes data from Canvas API")
    try:
        data = endpoints.get_quizzes(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} quizzes")

        # Log sample results
        log_json_results(data, "Quizzes")

        return {"endpoint_name": "Quizzes", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch quizzes: {str(e)}")
        raise


@task(name="fetch-canvas-quiz-submissions", description="Fetch all quiz submissions from Canvas", retries=2,
      retry_delay_seconds=30)
def fetch_quiz_submissions_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching quiz submissions data from Canvas API")
    try:
        data = endpoints.get_quiz_submissions(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} quiz submissions")

        # Log sample results
        log_json_results(data, "Quiz Submissions")

        return {"endpoint_name": "Quiz Submissions", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch quiz submissions: {str(e)}")
        raise


@task(name="fetch-canvas-assignments", description="Fetch all assignments from Canvas", retries=2,
      retry_delay_seconds=30)
def fetch_assignments_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching assignments data from Canvas API")
    try:
        data = endpoints.get_assignments(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} assignments")

        # Log sample results
        log_json_results(data, "Assignments")

        return {"endpoint_name": "Assignments", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch assignments: {str(e)}")
        raise


@task(name="fetch-canvas-assignment-submissions", description="Fetch all assignment submissions from Canvas", retries=2,
      retry_delay_seconds=30)
def fetch_assignment_submissions_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching assignment submissions data from Canvas API")
    try:
        data = endpoints.get_assignment_submissions(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} assignment submissions")

        # Log sample results
        log_json_results(data, "Assignment Submissions")

        return {"endpoint_name": "Assignment Submissions", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch assignment submissions: {str(e)}")
        raise


@task(name="fetch-canvas-courses", description="Fetch all courses from Canvas", retries=2, retry_delay_seconds=30)
def fetch_courses_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching courses data from Canvas API")
    try:
        data = endpoints.get_courses(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} courses")

        # Log sample results
        log_json_results(data, "Courses")

        return {"endpoint_name": "Courses", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch courses: {str(e)}")
        raise


@task(name="fetch-canvas-course-enrollment", description="Fetch all course enrollments from Canvas", retries=2,
      retry_delay_seconds=30)
def fetch_course_enrollment_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching course enrollment data from Canvas API")
    try:
        data = endpoints.get_course_enrollment(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} course enrollments")

        # Log sample results
        log_json_results(data, "Course Enrollment")

        return {"endpoint_name": "Course Enrollment", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch course enrollments: {str(e)}")
        raise


@task(name="fetch-canvas-discussions", description="Fetch all discussion topics from Canvas", retries=2,
      retry_delay_seconds=30)
def fetch_discussions_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching discussions data from Canvas API")
    try:
        data = endpoints.get_discussion_topics(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} discussion topics")

        # Log sample results
        log_json_results(data, "Discussions")

        return {"endpoint_name": "Discussions", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch discussion topics: {str(e)}")
        raise


@task(name="fetch-canvas-discussion-entries", description="Fetch all discussion entries from Canvas", retries=2,
      retry_delay_seconds=30)
def fetch_discussion_entries_data(api_client: APIClient) -> Dict[str, Any]:
    logger = get_run_logger()
    logger.info("Fetching discussion entries data from Canvas API")
    try:
        data = endpoints.get_discussion_entries(api_client)
        count = len(data) if isinstance(data, list) else 1
        logger.info(f"Successfully fetched {count} discussion entries")

        # Log sample results
        log_json_results(data, "Discussion Entries")

        return {"endpoint_name": "Discussion Entries", "data": data, "record_count": count, "status": "success"}
    except Exception as e:
        logger.error(f"Failed to fetch discussion entries: {str(e)}")
        raise


@task(name="save-users-to-db", description="Save users data to PostgreSQL database", retries=1)
def save_users_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    """
    Save users data to database
    load_mode: 'replace' (default), 'append', or 'fail'
    """
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_users"

    logger.info(f"Saving {record_count} users records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            # Normalize data to handle nested objects
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)

            # Prepare DataFrame for database insertion
            df_prepared = prepare_dataframe_for_db(df)

            # Save to database with specified load mode
            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} users records to {table_name}")

            return {
                "endpoint_name": "Users",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for users")
            return {
                "endpoint_name": "Users",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save users data to database: {str(e)}")
        raise


@task(name="save-quizzes-to-db", description="Save quizzes data to PostgreSQL database", retries=1)
def save_quizzes_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_quizzes"

    logger.info(f"Saving {record_count} quizzes records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            # Normalize data to handle nested objects
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)

            # Prepare DataFrame for database insertion
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} quizzes records to {table_name}")

            return {
                "endpoint_name": "Quizzes",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for quizzes")
            return {
                "endpoint_name": "Quizzes",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save quizzes data to database: {str(e)}")
        raise


@task(name="save-quiz-submissions-to-db", description="Save quiz submissions data to PostgreSQL database", retries=1)
def save_quiz_submissions_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_quiz_submissions"

    logger.info(f"Saving {record_count} quiz submissions records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} quiz submissions records to {table_name}")

            return {
                "endpoint_name": "Quiz Submissions",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for quiz submissions")
            return {
                "endpoint_name": "Quiz Submissions",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save quiz submissions data to database: {str(e)}")
        raise


@task(name="save-assignments-to-db", description="Save assignments data to PostgreSQL database", retries=1)
def save_assignments_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_assignments"

    logger.info(f"Saving {record_count} assignments records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} assignments records to {table_name}")

            return {
                "endpoint_name": "Assignments",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for assignments")
            return {
                "endpoint_name": "Assignments",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save assignments data to database: {str(e)}")
        raise


@task(name="save-assignment-submissions-to-db", description="Save assignment submissions data to PostgreSQL database",
      retries=1)
def save_assignment_submissions_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_assignment_submissions"

    logger.info(
        f"Saving {record_count} assignment submissions records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} assignment submissions records to {table_name}")

            return {
                "endpoint_name": "Assignment Submissions",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for assignment submissions")
            return {
                "endpoint_name": "Assignment Submissions",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save assignment submissions data to database: {str(e)}")
        raise


@task(name="save-courses-to-db", description="Save courses data to PostgreSQL database", retries=1)
def save_courses_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_courses"

    logger.info(f"Saving {record_count} courses records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} courses records to {table_name}")

            return {
                "endpoint_name": "Courses",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for courses")
            return {
                "endpoint_name": "Courses",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save courses data to database: {str(e)}")
        raise


@task(name="save-course-enrollment-to-db", description="Save course enrollment data to PostgreSQL database", retries=1)
def save_course_enrollment_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_course_enrollment"

    logger.info(f"Saving {record_count} course enrollment records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} course enrollment records to {table_name}")

            return {
                "endpoint_name": "Course Enrollment",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for course enrollment")
            return {
                "endpoint_name": "Course Enrollment",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save course enrollment data to database: {str(e)}")
        raise


@task(name="save-discussions-to-db", description="Save discussions data to PostgreSQL database", retries=1)
def save_discussions_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_discussions"

    logger.info(f"Saving {record_count} discussions records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} discussions records to {table_name}")

            return {
                "endpoint_name": "Discussions",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for discussions")
            return {
                "endpoint_name": "Discussions",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save discussions data to database: {str(e)}")
        raise


@task(name="save-discussion-entries-to-db", description="Save discussion entries data to PostgreSQL database",
      retries=1)
def save_discussion_entries_to_db(fetch_result: Dict[str, Any], load_mode: str = "replace") -> Dict[str, Any]:
    logger = get_run_logger()
    data = fetch_result["data"]
    record_count = fetch_result["record_count"]
    table_name = "canvas_discussion_entries"

    logger.info(f"Saving {record_count} discussion entries records to database table: {table_name} (mode: {load_mode})")

    try:
        engine = create_engine(get_db_connection_string())

        if isinstance(data, list) and len(data) > 0:
            normalized_data = normalize_json_data(data)
            df = pd.DataFrame(normalized_data)
            df_prepared = prepare_dataframe_for_db(df)

            df_prepared.to_sql(table_name, engine, if_exists=load_mode, index=False, method='multi')

            logger.info(f"Successfully saved {len(df_prepared)} discussion entries records to {table_name}")

            return {
                "endpoint_name": "Discussion Entries",
                "table_name": table_name,
                "record_count": len(df_prepared),
                "load_mode": load_mode,
                "status": "saved"
            }
        else:
            logger.warning("No data to save for discussion entries")
            return {
                "endpoint_name": "Discussion Entries",
                "table_name": table_name,
                "record_count": 0,
                "load_mode": load_mode,
                "status": "no_data"
            }

    except Exception as e:
        logger.error(f"Failed to save discussion entries data to database: {str(e)}")
        raise


@flow(
    name="canvas-data-extraction",
    description="Extract data from Canvas API endpoints in parallel and save to PostgreSQL database",
    task_runner=ConcurrentTaskRunner()
)
def canvas_data_extraction_flow(connection_info: Dict[str, Any] = CONNECTION_INFO, load_mode: str = "append") -> List[
    Dict[str, Any]]:
    logger = get_run_logger()
    logger.info("Starting Canvas data extraction flow")

    # Test database connection first
    test_database_connection()

    api_client = initialize_api_client(connection_info)

    # Submit all fetch tasks for parallel execution
    users_future = fetch_users_data.submit(api_client)
    quizzes_future = fetch_quizzes_data.submit(api_client)
    quiz_submissions_future = fetch_quiz_submissions_data.submit(api_client)
    assignments_future = fetch_assignments_data.submit(api_client)
    assignment_submissions_future = fetch_assignment_submissions_data.submit(api_client)
    courses_future = fetch_courses_data.submit(api_client)
    course_enrollment_future = fetch_course_enrollment_data.submit(api_client)
    discussions_future = fetch_discussions_data.submit(api_client)
    discussion_entries_future = fetch_discussion_entries_data.submit(api_client)

    logger.info("Submitted all fetch tasks for parallel execution")

    save_results = []

    # Process results and save to database
    users_result = users_future.result()
    if users_result["status"] == "success":
        save_result = save_users_to_db(users_result, load_mode)
        save_results.append(save_result)

    quizzes_result = quizzes_future.result()
    if quizzes_result["status"] == "success":
        save_result = save_quizzes_to_db(quizzes_result, load_mode)
        save_results.append(save_result)

    quiz_submissions_result = quiz_submissions_future.result()
    if quiz_submissions_result["status"] == "success":
        save_result = save_quiz_submissions_to_db(quiz_submissions_result, load_mode)
        save_results.append(save_result)

    assignments_result = assignments_future.result()
    if assignments_result["status"] == "success":
        save_result = save_assignments_to_db(assignments_result, load_mode)
        save_results.append(save_result)

    assignment_submissions_result = assignment_submissions_future.result()
    if assignment_submissions_result["status"] == "success":
        save_result = save_assignment_submissions_to_db(assignment_submissions_result, load_mode)
        save_results.append(save_result)

    courses_result = courses_future.result()
    if courses_result["status"] == "success":
        save_result = save_courses_to_db(courses_result, load_mode)
        save_results.append(save_result)

    course_enrollment_result = course_enrollment_future.result()
    if course_enrollment_result["status"] == "success":
        save_result = save_course_enrollment_to_db(course_enrollment_result, load_mode)
        save_results.append(save_result)

    discussions_result = discussions_future.result()
    if discussions_result["status"] == "success":
        save_result = save_discussions_to_db(discussions_result, load_mode)
        save_results.append(save_result)

    discussion_entries_result = discussion_entries_future.result()
    if discussion_entries_result["status"] == "success":
        save_result = save_discussion_entries_to_db(discussion_entries_result, load_mode)
        save_results.append(save_result)

    successful_saves = len(save_results)
    total_records = sum(r["record_count"] for r in save_results if r["status"] == "saved")

    logger.info(f"Canvas data extraction completed successfully")
    logger.info(f"Endpoints processed: {successful_saves}/9")
    logger.info(f"Total records saved: {total_records}")

    return save_results


def github_deploy():
    """
        Deploy the flow to Prefect Cloud using GitHub as the source repository.
        Not in use now
    """
    from prefect.runner.storage import GitRepository
    from prefect_github import GitHubCredentials
    from prefect_github import GitHubCredentials

    github_credentials_block = GitHubCredentials(token=os.getenv("GITHUB_TOKEN"))
    github_credentials_block.save(name="github-credentials-block")

    source = GitRepository(
        url="https://github.com/Andrew-Beard/evisions-prefect",
        credentials=GitHubCredentials.load("github-credentials-block")
    )
    flow.from_source(source=source, entrypoint="canvas_flow.py:canvas_data_extraction_flow").deploy(
        name="canvas_data_extraction_flow",
        work_pool_name="canvas-pool",
    )


if __name__ == "__main__":
    result = canvas_data_extraction_flow()

    print("\n" + "=" * 60)
    print("CANVAS DATA EXTRACTION SUMMARY")
    print("=" * 60)

    for save_result in result:
        endpoint = save_result["endpoint_name"]
        records = save_result["record_count"]
        table_name = save_result.get("table_name", "N/A")
        status = save_result["status"]
        load_mode = save_result.get("load_mode", "N/A")

        status_icon = "✅" if status == "saved" else "⚠️"
        print(f"{status_icon} {endpoint}: {records} records → {table_name} ({load_mode})")

    print(f"\nTotal tables updated: {len(result)}")
    print("=" * 60)
