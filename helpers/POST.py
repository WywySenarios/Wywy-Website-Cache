import requests

session = requests.Session()

# GET to receive CSRF cookie
session.get("http://127.0.0.1:8000/main/")
csrf_token = session.cookies.get("csrftoken")

headers = {
    "X-CSRFToken": csrf_token,
    "Content-Type": "application/json",
}

response = session.post(
    "http://127.0.0.1:8000/main/WywyWebsite/daily",
    json={
        "Date":"'0001-01-01T01:00:00'",
        "Work Ethic":0,
        "Time Efficiency":0,
        "Time Efficiency_comments": "'heya'",
        "Happiness":0,
        "Awareness":0,
        "Sleep Quality":0,
        "Sleep Behaviour":0,
        "Bedtime":"'03:30:00.000'",
        "Awakening":"'12:00:00.000'",
        "Productivity":"",
        "Social":"",
        "Exercise":"",
        "Computer":"",
        "Phone":"",
        "Awake":"",
        "Light Sleep":"",
        "Deep Sleep":"",
        "Record Time":0},
    headers=headers
)

print(response.status_code, response.text)
