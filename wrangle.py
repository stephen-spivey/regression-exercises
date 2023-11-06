import os
import pandas as pd
import env

filename = "zillow.csv"
db = "Zillow"
query = '''SELECT bathroomcnt, 
            calculatedfinishedsquarefeet, 
            yearbuilt, 
            taxamount, 
            fips, 
            taxvaluedollarcnt
            FROM properties_2017'''
    
def get_db_url(db, username=env.username, host=env.host, password=env.password):
    return f'mysql+pymysql://{username}:{password}@{host}/{db}'
    
def get_zillow_data(filename, query, db):

    if os.path.isfile(filename):

        return pd.read_csv(filename)
    else:
        # Create the url
        url = get_db_url(db)

        # Read the SQL query into a dataframe
        df = pd.read_sql(query, url)

        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename)

        # Return the dataframe to the calling code
        return df

    