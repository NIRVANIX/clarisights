from mongodb import MongoDB
from csv import reader
from datetime import datetime


def upload_file(file_path, db_name):
	mongodb = MongoDB(db_name)
	mongo_client = mongodb.client()
	db = mongo_client[db_name]

	with open(file_path, 'r') as read_obj:
		csv_reader = reader(read_obj)
		header = next(csv_reader)
		for row in csv_reader:
			date_object = datetime.strptime(row[0], '%Y-%m-%d')
			document = {
				'_id' : row[0] + "_" + row[1],
				'date' : date_object,
				'query' : row[1],
				'count' : float(row[2])
			}
			db.google_ads.insert_one(document)

if __name__ == '__main__':
	file_name = "/home/neel/clarisights/sample+data.csv"
	db = "clarisights"

	upload_file(file_name, db)