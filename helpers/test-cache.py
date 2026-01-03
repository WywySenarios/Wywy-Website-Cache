import requests

BASE_URL = "http://localhost:8000/cache/wywywebsite/events"

session = requests.Session()

# 1. Initial GET to receive CSRF cookie
get_response = session.get(BASE_URL)
print("GET status:", get_response.status_code)
print("Cookies after GET:", session.cookies.get_dict())

csrf_token = session.cookies.get("csrftoken")
if not csrf_token:
    raise RuntimeError("CSRF token not set")

# 2. POST using the CSRF token with manual JSON body
post_headers = {
    "X-CSRFToken": csrf_token,
    "Content-Type": "application/json",
}

post_body = {
    "start_time": "'2026-01-03T02:36:34'"
}

post_response = session.post(
    BASE_URL,
    headers=post_headers,
    json=post_body,  # <-- manual body
)

print("POST status:", post_response.status_code)
print("POST response:", post_response.text)

# 3. GET again
second_get = session.get(BASE_URL)
print("Second GET status:", second_get.status_code)
print("Second GET response:", second_get.text)
