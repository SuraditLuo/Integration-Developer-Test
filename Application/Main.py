import requests
from dotenv import load_dotenv
import os
import json
import datetime
import psycopg2
import notion_client

load_dotenv()

database_id = os.getenv("NOTION_DATABASE_ID")
tasks_db_id = os.getenv("NOTION_TASK_DATABASE_ID")
notion_api = os.getenv("NOTION_KEY")
postgres_port = os.getenv("POSTGRES_PORT")
psql_password = os.getenv("POSTGRES_PASSWORD")
headers = {
    "Authorization": "Bearer " + notion_api,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def extract_task_info(data):
    info = {id: None,'task_name': "", 'status': None, 'due_date': datetime}
    properties = data.get("properties", {})
    for key, value in properties.items():
        if key == "ID":
            info["id"] = value["number"]
        elif key == "Task Name":
            info["task_name"] = value["title"][0]["plain_text"]
        elif key == "Status":
            info["status"] = value["status"]["name"]
        elif key == "Due Date":
            timestamp = datetime.datetime.strptime(value["date"]["start"],"%Y-%m-%dT%H:%M:%S.%f%z")
            info["due_date"] = timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
    return info if info else None

def insert_into_database(task):
    sql = """
        INSERT INTO tasks (id, task_name, status, due_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """
    values = [task['id'], task['task_name'], task['status'], task['due_date']]
    conn = psycopg2.connect(
        dbname="notiondb",
        user="postgres",
        password=psql_password,
        host="localhost",
        port=postgres_port 
    )
    cursor = conn.cursor()
    cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

def get_database():
    url = f"https://api.notion.com/v1/databases/{tasks_db_id}/query"
    page_size = 100

    payload = {"page_size": page_size}
    response = requests.post(url, json=payload, headers=headers)

    data = response.json()
    filename = "output.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(data["results"])
    results = [extract_task_info(item) for item in data["results"]]
    for result in results:
        insert_into_database(result)
        print(result)

def create_notion_page(db_id):
    with open('Application\mock_data.Json', 'r') as f:
        properties = json.load(f)
    try:
        # Authenticate with your Notion API token
        client = notion_client.Client(auth=notion_api)

        # Create the new page
        response = client.pages.create(
            parent={"type": "database_id", "database_id": db_id},
            properties=properties.get("properties", {})
        )
        print("Page created successfully.")
        #update database
        result = extract_task_info(properties)
        insert_into_database(result)
        return response
    except Exception as e:
        print(f"Error creating page: {e}")
        return None
if __name__ == '__main__':
    # get_database()
    create_notion_page(tasks_db_id)