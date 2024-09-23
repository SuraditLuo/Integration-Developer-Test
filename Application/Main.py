import requests
from dotenv import load_dotenv
import os
import json
import datetime
import psycopg2
import notion_client
import traceback
load_dotenv()

database_id = os.getenv("NOTION_DATABASE_ID")
tasks_db_id = os.getenv("NOTION_TASK_DATABASE_ID")
notion_api = os.getenv("NOTION_KEY")
postgres_port = os.getenv("POSTGRES_PORT")
psql_password = os.getenv("POSTGRES_PASSWORD")
page_id = os.getenv("NOTION_PAGE_TWO_ID")
headers = {
    "Authorization": "Bearer " + notion_api,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def extract_task_info(data):
    info = {id: None,'task_name': "", 'status': None, 'due_date': datetime, 'updated_at': datetime}
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
        elif key == "Updated At":
            update_timestamp = datetime.datetime.strptime(value["date"]["start"],"%Y-%m-%dT%H:%M:%S.%f%z")
            info["updated_at"] = update_timestamp.strftime("%Y-%m-%d %H:%M:%S%z")
            
    return info if info else None

def insert_into_database(task):
    sql = """
        INSERT INTO tasks (id, task_name, status, due_date, updated_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """
    values = [task['id'], task['task_name'], task['status'], task['due_date'], task['updated_at']]
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

def update_task_name(task):
    values = [task['task_name'], task['updated_at'], task['id']]
    print(values)
    sql = """
        UPDATE tasks
        SET 
        task_name = %s,
        updated_at = %s
        WHERE id = %s;
    """
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
    #see database as JSON improve data visibility
    filename = "output.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    return data

def update_database():
    data = get_database()
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
        task = extract_task_info(properties)
        print(task)
        # Create the new page
        data = get_database()
        results = [extract_task_info(item) for item in data["results"]]
        #Check for id conflict on Notion databases.
        for result in results:
            if result['id'] == task['id']:
                print("page already exist")
                exit()
        response = client.pages.create(
            parent={"type": "database_id", "database_id": db_id},
            properties=properties.get("properties", {})
        )
        print("Page created successfully.")
        #update database
        insert_into_database(task)
        return response
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)[-1]
        print(f"Error occurred on line {tb.lineno}")
        print(f"Error creating page: {e}")
        return None

def update_notion_page(page_id: str):
    now = datetime.datetime.now()
    formatted_time = now.strftime("%Y-%m-%dT%H:%M:%S.%f+07:00")
    try:
        # Authenticate with your Notion API token
        url = f"https://api.notion.com/v1/pages/{page_id}"
        #Mock data
        data = {
                'properties':{
                    'ID': {"type": "number", "number": 2}, 
                    'Task Name': {"type": "title", "title": [{"type": "text","text": {"content": "Implement the 2nd feature"}, 'plain_text': "Implement the 2nd feature"}]}, 
                    'Updated At': {"type": "date", "date": {"start": formatted_time, "end": None}}
                    }
                }
        # Exclude the 'ID' property
        new_data = {k: v for k, v in data['properties'].items() if k != 'ID'}
        payload = {"properties": new_data}
        res = requests.patch(url, json=payload, headers=headers)
        print(res.status_code)
        #update sql database
        updated_data = extract_task_info(data)
        update_task_name(updated_data)
        return res
        #update sql row 
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)[-1]
        print(f"Error occurred on line {tb.lineno}")
        print(f"Error updating page: {e}")
        return None

if __name__ == '__main__':
    # get_database()

    # update_database()

    # create_notion_page(tasks_db_id)

    update_notion_page(page_id)