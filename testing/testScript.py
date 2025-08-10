import requests

response = requests.post(
    "http://127.0.0.1:8000/query",
    json={"query": "how many items were purchased by the user named First26"}
)

print(response.json())

""" response = requests.post(
    "http://127.0.0.1:8000/nosql-query",
    json={"query": "how many notepads were sold"}
)

print(response.json()) """