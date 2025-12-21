import requests

session = requests.Session()

# GET to receive CSRF cookie
session.get("http://127.0.0.1:8000/main/")
csrf_token = session.cookies.get("csrftoken")

headers = {
    "X-CSRFToken": csrf_token
}

response = session.post(
    "http://127.0.0.1:8000/main/",
    json={"hello": "world"},
    headers=headers
)

print(response.status_code, response.text)
