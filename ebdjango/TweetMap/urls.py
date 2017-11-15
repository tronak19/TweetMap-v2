from django.conf.urls import url
from . import views

app_name = 'TweetMap'

urlpatterns = [
	url(r'^$', views.index, name='index'),
	url(r'^ajax_process$', views.ajax_process, name='ajax_process'),
	url(r'^search_query$', views.search_query, name='search_query'),
]