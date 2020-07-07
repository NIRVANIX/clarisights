from csv import reader
from datetime import datetime

import pandas as pd

def ads_data(start_date, end_date, dimensions, metrics, mongo_client_db):
		db = mongo_client_db

		select_dict = {'_id' : 0}
		for dim in dimensions:
			select_dict[dim] = 1
		for metric in metrics:
			select_dict[metric] = 1

		start_date = datetime.strptime(start_date, '%Y-%m-%d')
		end_date = datetime.strptime(end_date, '%Y-%m-%d')
		
		result = db.google_ads.find(
			{
				"date": {"$gte" : start_date},
				"date": {"$lte" : end_date}
			},
			select_dict
		)

		df = pd.DataFrame(result)
		grouped_df = df.groupby(dimensions).sum().reset_index()
		
		result_dict = {}

		for metric in metrics:
			result_df = grouped_df.loc[grouped_df[metric] >= 50]
			result_df = result_df.append({'query' : 'Queries with count < 50',
		 				'count' : sum(grouped_df.loc[grouped_df[metric] < 50][metric])} , ignore_index=True)
			result_dict[metric] = result_df.to_dict('records')
		
		return result_dict
