from django.http import (
    HttpResponse,
    HttpRequest,
    HttpResponseBadRequest,
    HttpResponseServerError,
    HttpResponseNotAllowed,
)
from .creds import check_creds
from .sessions import create_session
from typing import cast, Any

import json


def index(request: HttpRequest) -> HttpResponse:
    # only allow POST requests with JSON payload
    if request.method != "POST":
        return HttpResponseNotAllowed(["POST"])
    if request.content_type != "application/json":
        return HttpResponseBadRequest("Expected application/json.")

    # is the json valid?
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError as e:
        return HttpResponseBadRequest(f"Invalid JSON: {e}")

    if not isinstance(data, dict):
        return HttpResponseBadRequest("Not a JSON object.")
    data = cast(dict[str, Any], data)
    username = data.get("username")
    password = data.get("password")
    if not isinstance(username, str) or not isinstance(password, str):
        return HttpResponseBadRequest("Missing or non-string username or password.")

    # check creds
    if check_creds(username, password):
        token = create_session(username)
        if token is not None:
            response = HttpResponse("Login successful!")
            response.set_cookie(
                "token", token, secure=True, httponly=True, samesite="Lax"
            )
            return response
        else:
            return HttpResponseServerError()
    else:
        return HttpResponse("Login failed.", status=401)
