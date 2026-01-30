from django.shortcuts import render
from django.http import QueryDict, HttpResponse, HttpRequest, HttpResponseBadRequest, HttpResponseForbidden

from os import environ
import json

def index(request: HttpRequest) -> HttpResponse:
    with open("/run/secrets/admin", "r") as f:
        password: str = f.read()
        
    # only allow POST requests with JSON payload
    if not request.method == "POST" or not request.content_type == "application/json":
        return HttpResponseBadRequest()
    
    # is the json valid?
    data = json.loads(request.body)
    if data is None or "username" not in data or "password" not in data:
        return HttpResponseBadRequest()

    # check creds
    if data["username"] != "admin" or data["password"] != password:
        return HttpResponseForbidden()
    
    response: HttpResponse = HttpResponse()
    response.set_cookie("username", "admin", max_age=int(environ["AUTH_COOKIE_MAX_AGE"]))
    response.set_cookie("password", password, max_age=int(environ["AUTH_COOKIE_MAX_AGE"]))
    return response