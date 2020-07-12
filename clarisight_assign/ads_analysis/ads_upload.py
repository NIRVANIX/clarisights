from mongodb import MongoDB
from csv import reader
from datetime import datetime
from bson.objectid import ObjectId

def get_query_less_than_50(db_conn, query, count):
	result = db_conn.google_ads_less_50.find({'query' : query})
	return list(result)

def check_if_greater_than_50(db_conn, query):
	return db_conn.google_ads_50.find({'query' : query}).count()

def insert_documet_to_google_ads(db_conn, document):
	db_conn.google_ads.insert_one(document)

def insert_documet_to_google_ads_50(db_conn, query):
	db_conn.google_ads_50.insert_one({'query' : query})

def insert_documet_to_google_ads_less_50(db_conn, document):
	db_conn.google_ads_less_50.insert_one(document)

def delete_document_from_google_ads_less_50(db_conn, _id):
	db_conn.google_ads_less_50.delete_one({'_id' : _id})

def upload_document(document, query, count, db_conn):

	if check_if_greater_than_50(db_conn, query) > 0:
		insert_documet_to_google_ads(db_conn, document)
	else:
		query_list = get_query_less_than_50(db_conn, query, count)

		count_sum = 0
		for doc in query_list:
			count_sum += doc['count']

		if count_sum + count <= 50:
			insert_documet_to_google_ads_less_50(db_conn, document)
		else:
			for doc in query_list:
				insert_documet_to_google_ads(db_conn, doc)
				delete_document_from_google_ads_less_50(db_conn, doc["_id"])
			insert_documet_to_google_ads(db_conn, document)
			insert_documet_to_google_ads_50(db_conn, query)

def upload_file(file_path, db_name):
	mongodb = MongoDB(db_name)
	mongo_client = mongodb.client()
	db_conn = mongo_client[db_name]

	with open(file_path, 'r') as read_obj:
		csv_reader = reader(read_obj)
		header = next(csv_reader)
		for row in csv_reader:
			date_object = datetime.strptime(row[0], '%Y-%m-%d')
			query = row[1]
			count = float(row[2])
			document = {
				'_id' : row[0] + "_" + row[1],
				'date' : date_object,
				'query' : query,
				'count' : count
			}
			upload_document(document, query, count, db_conn)

if __name__ == '__main__':
	file_name = "/home/neel/clarisights/sample_test_1"
	db = "clarisights"
	upload_file(file_name, db)