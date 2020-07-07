from django.shortcuts import render

from .mongodb import MongoDB
from csv import reader
from rest_framework.response import Response
from rest_framework.views import APIView
from datetime import datetime

import pandas as pd

from .ads_processing import ads_data


class AdsView(APIView):

	# Sample call - /ads/view
	# {
 	#	"start_date" : 
 	#	"end_date" : 
 	#   "dimensions" :
 	#   "metrics" :
	# }	
	DB_NAME = "clarisights"


	def post(self, request, format=None):
		
		mongodb = MongoDB(self.DB_NAME)
		mongo_client = mongodb.client()
		mongo_client_db = mongo_client[self.DB_NAME]

		result = ads_data(request.data['from_date'],
			request.data['to_date'],
			request.data['dimensions'], 
			request.data['metrics'], mongo_client_db)
		
		return Response(result)
