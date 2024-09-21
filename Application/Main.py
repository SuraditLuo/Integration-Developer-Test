import flask
import requests
from dotenv import load_dotenv
import os
import pandas as pd
import json
import datetime
load_dotenv()
database_id = os.getenv("NOTION_DATABASE_ID")
task_id = os.getenv("NOTION_TASK_DATABASE_ID")
notion_api = os.getenv("NOTION_KEY")

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
            info["id"] = f"{value["unique_id"]["prefix"]}-{value["unique_id"]["number"]}"
        elif key == "Task Name":
            info["task_name"] = value["title"][0]["plain_text"]
        elif key == "Status":
            info["status"] = value["status"]["name"]
        elif key == "Due Date":
            info["due_date"] = datetime.datetime.strptime(value["date"]["start"],"%Y-%m-%d").timestamp
    return info if info else None

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
    print(results)
    results = [extract_task_info(item) for item in data["results"]]
    for result in results:
        print(result)

if __name__ == '__main__':
    get_database()