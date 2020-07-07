from pymongo import MongoClient

class MongoDB(object):
	HOST = "localhost"
	PORT = 27017

	def __init__(self, database):
		self.database = database

	def client(self):
		client = MongoClient(self.HOST,
							 self.PORT)
		return client
