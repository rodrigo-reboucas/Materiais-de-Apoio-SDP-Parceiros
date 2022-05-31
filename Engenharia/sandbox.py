import pandas as pd
from sqlalchemy import create_engine

conn = create_engine("trino://root@datalake-1:18080/hive")

# Example query
query = "select * from hive.service.imdb"

conn.execute(query)

df = pd.read_sql_query(con=conn, sql=query)

df.to_sql(name='pandas_imdb', con=conn, schema='service', index=False, method='multi', if_exists='replace')