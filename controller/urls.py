from django.urls import path, include
from . import views

app_name = 'controller'
urlpatterns = [
    # ex: /controller/
    path('', views.process, name='process'),


]