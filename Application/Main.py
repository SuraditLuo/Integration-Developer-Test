import requests
from dotenv import load_dotenv
import os
import json
import datetime
import psycopg2
load_dotenv()
database_id = os.getenv("NOTION_DATABASE_ID")
task_id = os.getenv("NOTION_TASK_DATABASE_ID")
notion_api = os.getenv("NOTION_KEY")
postgres_port = os.getenv("POSTGRES_PORT")
psql_password = os.getenv("POSTGRES_PASSWORD")
headers = {
    "Authorization": "Bearer " + notion_api,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def extract_task_info(data):
    info = {'id': None, 'task_name': "", 'status': None, 'due_date': datetime}
    properties = data.get("properties", {})
    for key, value in properties.items():
        if key == "ID":
            info["id"] = f"{value["unique_id"]["number"]}"
        elif key == "Task Name":
            info["task_name"] = value["title"][0]["plain_text"]
        elif key == "Status":
            info["status"] = value["status"]["name"]
        elif key == "Due Date":
            timestamp = datetime.datetime.strptime(value["date"]["start"],"%Y-%m-%dT%H:%M:%S.%f%z")
            info["due_date"] = timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
    return info if info else None

def insert_into_database(tasks):
    delete = """
        DELETE FROM public.task;
    """
    sql = """
        INSERT INTO task (id, task_name, status, due_date)
        VALUES (%s, %s, %s, %s);
    """
    conn = psycopg2.connect(
        dbname="notiondb",
        user="postgres",
        password=psql_password,
        host="localhost",
        port=postgres_port 
    )
    cursor = conn.cursor()
    cursor.execute(delete)
    for task in tasks:
        values = [task['id'], task['task_name'], task['status'], task['due_date']]
        cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

def get_database():
    url = f"https://api.notion.com/v1/databases/{task_id}/query"
    page_size = 100

    payload = {"page_size": page_size}
    response = requests.post(url, json=payload, headers=headers)

    data = response.json()
    results = data["results"]
    filename = "output.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    results = [extract_task_info(item) for item in data["results"]]
    insert_into_database(results)

if __name__ == '__main__':
    get_database()