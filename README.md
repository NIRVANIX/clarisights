# clarisights
Lang - Python + Django

DB - MongoDB

File to upload csv input to mongo DB -  
clarisight_assign/ads_analysis/ads_upload.py
Method - upload_file
Method takes file_path and DB as input. It reads each line of the file and inserts into DB

API to view data for query + count -
clarisight_assign/ads_analysis/views.py

Logic for fetching and processing data - 
clarisight_assign/ads_analysis/ads_processing.py
Method - ads_data
Method takes start_date, end_date, dimensions and metrics as input. 
Fetch the Data (Dimentsions + metrics cols) from DB with where date between start_date and end_date. Group by the result on dimensions with sum applied over metrics columns. Returns a json with output for each metric as key and value as dimensions and metric sum value.

Test file - 
/clarisight_assign/ads_analysis/tests.py
