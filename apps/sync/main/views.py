from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import JsonResponse

@ensure_csrf_cookie
def index(request):
    if request.method == "POST":
        return JsonResponse({"ok": True})
    
    return JsonResponse({"detail": "csrf cookie set"})