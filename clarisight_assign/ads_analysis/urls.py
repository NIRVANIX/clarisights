from django.urls import path

from . import views

urlpatterns = [
    path('view', views.AdsView.as_view(), name='ads_view'),
]