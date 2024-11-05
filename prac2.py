
import datetime
import pandas as pd
from datetime import timedelta
import requests
import os
from google.cloud import storage




def upload_to_gcs(bucket_name,destination_file_path,source_file_name):
    #    use storage.client to fetch destination bucket
       storage_client = storage.Client()

       bucket= storage_client.bucket(bucket_name)

       _file_destination=bucket.blob(destination_file_path)

       upload_to_destination=_file_destination.upload_from_filename(source_file_name)
       
    



def fetch():
    # Set dates
    today = datetime.date.today()
    start_date = today - timedelta(days=1)
    end_date = today

#     # API key and URL
    api_key = "f5ff29fa63984ba29f75e0700fade3a9"
    base_url = f"https://newsapi.org/v2/everything?q=tesla&from={start_date}&to={end_date}&sortBy=publishedAt&apiKey={api_key}"

    # Make the API request
    response = requests.get(base_url)   
#     # we are using .json() to convert json format data into python dictonary  .Python because it makes the data easier to work with programmatically.
#     #Python dictionaries naturally support operations such as:

#     # Retrieving values using keys
#     # Handling missing keys with .get()
#     # Looping through key-value pairs, which is useful when working with large datasets from APIs

    d = response.json()    

#     # Initialize DataFrame with columns
    df = pd.DataFrame(columns=['Author_name', 'Author_title', 'Source_name', 'TimeStamp', 'Content'])

    # Loop through articles to extract data

    # we are using .get method  to handle errors if data is not present .get method return null if data is not present we can handle with if else also but it will still throw error
    for i in d.get('articles', []):
        Author_name = i.get('author', "")  
        Author_title = i.get('title', "")
        Source_name = i['source'].get('name', "")
        TimeStamp = i.get('publishedAt', "")
        Content = i.get('content', "")

        # Check and trim content length if necessary
        if len(Content) > 200:
            Content = Content[:200]

        # Create a temporary DataFrame for the current article
        df1 = pd.DataFrame({
            'Author_name': [Author_name],
            'Author_title': [Author_title],
            'Source_name': [Source_name],
            'TimeStamp': [TimeStamp],
            'Content': [Content]
        })

        # Concatenate the new data to the main DataFrame
        df = pd.concat([df, df1], ignore_index=True)


         # creating unique filesname
        currenttime=datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        file_name=f'run_{currenttime}.parquet'
        print(df)
         
        
        
        # saving the dataframe and their data in parquet form using pandas to parquet method
        # now our file is present in local storage

        print("Current Working Directory:", os.getcwd())

        df.to_parquet(file_name)
       

    # uploading parquet file from local to gcs bucket  by using import 

    bucket_name="cloudies"
    # providing destinaton file path jaha apn ko file upload krni hai
        
    destination_file_path=f'SNOWFLAKE_PROJECT/PARQUET_FILES/{file_name}'

    upload_to_gcs(bucket_name,destination_file_path,file_name)
     

     

         

        
        



        


    




    


