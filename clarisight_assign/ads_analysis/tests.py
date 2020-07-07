import unittest

from mongodb import MongoDB
from ads_upload import upload_file
from ads_processing import ads_data

def get_test_data_count():
	mongodb = MongoDB("test_clarisights")
	mongo_client = mongodb.client()
	db = mongo_client["test_clarisights"]
	count = db.google_ads.count()
	return count

class TestAds(unittest.TestCase):

	def test_ads_upload(self):
		file_path = "/home/neel/clarisights/test.csv"
		db = "test_clarisights"
		upload_file(file_path, db)
		count = get_test_data_count()
		self.assertEquals(count,4)
	
	def test_ads_view(self):
		mongodb = MongoDB("test_clarisights")
		mongo_client = mongodb.client()
		mongo_client_db = mongo_client["test_clarisights"]

		result_dict = ads_data("2020-06-01", "2020-06-01", ["query"], ["count"], mongo_client_db)
		self.assertEquals(result_dict['count'][0]['count'], 2000)
		mongo_client_db.google_ads.drop()

if __name__ == '__main__':
   unittest.main()
