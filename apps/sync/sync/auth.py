from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseForbidden,
)
from typing import Callable


class AuthMiddleware:
    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]) -> None:
        self.get_response = get_response

        with open("/run/secrets/admin", "r") as f:
            self.password = f.read()

    def __call__(self, request: HttpRequest) -> HttpResponse:
        # @TODO session cookies

        # check creds. Deny access if creds are invalid.
        # The authentication endpoint does not need authentication (avoid chicken & egg scenario).
        # This includes CSRF tokens!

        if request.path.startswith("/auth") or request.path.startswith("/cache/csrf"):
            return self.get_response(request)

        username_supplied: bool = "username" in request.COOKIES
        password_supplied: bool = "password" in request.COOKIES
        if not username_supplied:
            if not password_supplied:
                return HttpResponse("No credentials supplied.", status=401)
            else:
                return HttpResponseForbidden("Invalid credentials.")
        if not password_supplied:
            return HttpResponseForbidden("Invalid credentials")

        if (
            request.COOKIES["username"] != "admin"
            or request.COOKIES["password"] != self.password
        ):
            return HttpResponseForbidden("Invalid credentials.")

        return self.get_response(request)
