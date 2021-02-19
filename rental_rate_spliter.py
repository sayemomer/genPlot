import sqlalchemy
import pandas as pd

# Connect to the database using the connection URI
connection_uri = "postgresql://omer:123@localhost:5432/test" 
db_engine = sqlalchemy.create_engine(connection_uri)

# Function to extract table to a pandas DataFrame
def extract_table_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    return pd.read_sql(query, db_engine)

def transform_rental_rate(film_df):
    #Get the rental rate column as a string
    rental_rate_str = film_df.rental_rate.astype('str')

    rental_rate_expanded = rental_rate_str.str.split('.', expand=True)

    film_df = film_df.assign(
        rental_rate_dollar=rental_rate_expanded[0],
        rental_rate_cents=rental_rate_expanded[1],
    )

    return film_df

def load_dataframe_to_film(film_df):
    film_df.to_sql("t_film",db_engine,if_exists="append", index=True)


def etl():
    film_df = extract_table_to_pandas("film",db_engine)
    film_df = transform_rental_rate(film_df)
    load_dataframe_to_film(film_df)
    
