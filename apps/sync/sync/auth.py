from django.http import HttpRequest, HttpResponse, HttpResponseForbidden

class AuthMiddleware:
    def __init__(self, get_response) -> None:
        self.get_response = get_response
        
        with open("/run/secrets/admin", "r") as f:
            self.password = f.read()
    
    def check_creds(self, request: HttpRequest) -> bool:
        """Checks if the user has admin privileges. @TODO sessions

        Args:
            request (HttpRequest): The request from the user in question.

        Returns:
            bool: Whether or not the user has admin privileges.
        """
        if "username" not in request.COOKIES or "password" not in request.COOKIES:
            return False
        
        if request.COOKIES["username"] == "admin" and request.COOKIES["password"] == self.password:
            return True
        
        return False

    def __call__(self, request: HttpRequest) -> HttpResponse:
        
        # check creds. Deny access if creds are invalid.
        if self.check_creds(request):
            response = self.get_response(request)
        else:
            response = HttpResponseForbidden("Invalid credentials.")
        return response
    
    def process_view(self, request, view_func, view_args, view_kwargs) -> None | HttpResponse:
        # check creds. Deny access if creds are invalid.
        response: None | HttpResponse
        if self.check_creds(request):
            response = None
        else:
            response = HttpResponseForbidden("Invalid credentials.")
        
        return response