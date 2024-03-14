from dotenv import load_dotenv
import requests
import json
import os
import csv


def log_in_to_metabase(host, username, password):
    url = f"{host}/api/session"
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "username": username,
        "password": password
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    response.raise_for_status()
    return response.json()["id"]


def list_groups(host, token):
    url = f"{host}/api/permissions/group"
    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": token
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def list_users_in_group(host, token, group_id):
    url = f"{host}/api/permissions/group/{group_id}"
    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": token
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def list_collection_items(host, token, collection_id="root"):
    url = f"{host}/api/collection/{collection_id}/items"
    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": token
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_collection(host, token, collection_id):
    url = f"{host}/api/collection/{collection_id}"
    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": token
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def list_items_reccursive(host, token, parent_collection_id="root", result=[], ancestor_ids="", ancestor_names=""):
    parent_collection = get_collection(host, token, parent_collection_id)
    collections = list_collection_items(host, token, parent_collection_id)

    if parent_collection_id != "root":
        ancestor_ids += f"/{parent_collection_id}"
        ancestor_names += f"/{parent_collection['name']}"

    for item in collections['data']:
        if item["model"] == "collection":
            list_items_reccursive(
                host, token, item["id"], result, ancestor_ids, ancestor_names)
        else:
            result.append({
                "id": item["id"],
                "name": item["name"],
                "link": f"{host}/{item['model']}/{item['id']}",
                "ancestor_ids": ancestor_ids,
                "ancestor_names": ancestor_names,
                "last_edited_by": item["last-edit-info"]["email"],
                "last_edited_at": item["last-edit-info"]["timestamp"]
            })
    return result


def main():
    load_dotenv()
    token = log_in_to_metabase(
        os.getenv("METABSE_HOST"),
        os.getenv("METABSE_USER"),
        os.getenv("METABSE_PASSWORD")
    )

    # グループごとのユーザー一覧を出力
    groups = list_groups(os.getenv("METABSE_HOST"), token)
    with open("permissions.tsv", "w") as tsv_file:
        writer = csv.writer(tsv_file, quotechar='"',
                            quoting=csv.QUOTE_ALL, delimiter="\t")
        writer.writerow(["group", "user"])
        for group in groups:
            if group["name"] == "All Users":
                continue
            users = list_users_in_group(
                os.getenv("METABSE_HOST"), token, group["id"])
            [writer.writerow([
                group["name"],
                user["email"]
            ]) for user in users["members"]]

    # コレクションに保存されたクエリ一覧を出力
    with open("queries.tsv", "w") as tsv_file:
        writer = csv.writer(tsv_file, quotechar='"',
                            quoting=csv.QUOTE_ALL, delimiter="\t")
        writer.writerow(["id", "name", "link", "ancestor_ids",
                        "ancestor_names", "last_edited_by", "last_edited_at"])
        items = list_items_reccursive(os.getenv("METABSE_HOST"), token)
        for item in items:
            writer.writerow([
                item['id'],
                item['name'],
                item['link'],
                item['ancestor_ids'],
                item['ancestor_names'],
                item['last_edited_by'],
                item['last_edited_at']
            ])


if __name__ == "__main__":
    main()
