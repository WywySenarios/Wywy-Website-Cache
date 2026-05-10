from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseForbidden,
)
from typing import Callable
from .sessions import validate_session
from logging import getLogger

logger = getLogger("auth")


class AuthMiddleware:
    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]) -> None:
        self.get_response = get_response

        with open("/run/secrets/admin", "r") as f:
            self.password = f.read()

    def __call__(self, request: HttpRequest) -> HttpResponse:
        # The authentication endpoint and CSRF endpoint are unauthenticated (avoid chicken & egg scenario).
        if request.path.startswith("/auth") or request.path.startswith("/cache/csrf"):
            return self.get_response(request)

        if "token" not in request.COOKIES:
            return HttpResponse("No credentials supplied.", status=401)

        is_session_valid, username = validate_session(request.COOKIES["token"])

        logger.info(f"{request.COOKIES["token"]}: {is_session_valid}, {username}")

        if not is_session_valid:
            return HttpResponse("Invalid credentials.", status=401)

        if username != "admin":
            return HttpResponseForbidden("Insufficient permissions.")

        return self.get_response(request)
