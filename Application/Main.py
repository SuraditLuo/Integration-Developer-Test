import flask
import requests
from dotenv import load_dotenv
import os
import pandas as pd
import json
load_dotenv()
database_id = os.getenv("NOTION_DATABASE_ID")
notion_api = os.getenv("NOTION_KEY")
headers = {
    "Authorization": "Bearer " + notion_api,
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}
def get_database():
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    page_size = 100

    payload = {"page_size": page_size}
    response = requests.post(url, json=payload, headers=headers)

    data = response.json()
    results = data["results"]
    filename = "output.json"
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
    print(results)
if __name__ == '__main__':
    get_database()