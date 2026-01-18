from django.urls import re_path, path

from . import views

urlpatterns = [
    path("csrf", views.csrf, name='csrf'),
    re_path(r'^.*$', views.index, name='index'),
]