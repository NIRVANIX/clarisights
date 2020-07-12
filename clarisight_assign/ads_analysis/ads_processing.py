from csv import reader
from datetime import datetime

import pandas as pd

def get_ads_data(db_conn, start_date, end_date, select_dict):
	result = db_conn.google_ads.find(
			{
				"date": {"$gte" : start_date},
				"date": {"$lte" : end_date}
			},
			select_dict
		)
	return result

def get_count_ads_data_less_than_50(db_conn, start_date, end_date, select_dict):
	result = db_conn.google_ads_less_50.aggregate([
		{"$match": 
			{"$and": [
				{ "date": { "$gte": start_date } },
        		{ "date": { "$lte": end_date } }]
        	}
        },
		{"$group": 
			{
				"_id": 1,
				"all" : { "$sum": "$count"}
			}
		}]);
	
	result = list(result)
	if len(result) > 0:
		return result[0]['all']
	else:
		return 0

def ads_data(start_date, end_date, dimensions, metrics, db_conn):
		
		select_dict = {'_id' : 0}
		for dim in dimensions:
			select_dict[dim] = 1
		for metric in metrics:
			select_dict[metric] = 1

		start_date = datetime.strptime(start_date, '%Y-%m-%d')
		end_date = datetime.strptime(end_date, '%Y-%m-%d')
		
		result = get_ads_data(db_conn, start_date, end_date, select_dict)

		df = pd.DataFrame(result)
		result_df = df.groupby(dimensions).sum().reset_index()
		
		less_50_count = get_count_ads_data_less_than_50(db_conn, start_date, end_date, select_dict)
		
		result_df = result_df.append({'query' : 'Queries with count < 50',
		 			'count' : less_50_count} , ignore_index=True)

		result_dict = {}
		for metric in metrics:
			result_dict[metric] = result_df.to_dict('records')
		
		return result_dict
