from django.http import HttpRequest

def check_creds(request: HttpRequest) -> bool:
    """Checks if the user has admin privileges. @TODO sessions

    Args:
        request (HttpRequest): The request from the user in question.

    Returns:
        bool: Whether or not the user has admin privileges.
    """
    if "username" not in request.COOKIES or "password" not in request.COOKIES:
        return False
    
    with open("/run/secrets/admin", "r") as f:
        if request.COOKIES["username"] == "admin" and request.COOKIES["password"] == f.read():
            return True
    
    return False