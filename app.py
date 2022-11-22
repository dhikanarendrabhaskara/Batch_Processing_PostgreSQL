import connection   # the previous connection module
import os
import sqlparse
import pandas as pd

if __name__ == "__main__":
    print("[INFO] Service-ETL is Starting ... ")

    # connection datasource
    conf = connection.config('marketplace_prod')
    conn, engine = connection.psql_conn(conf, 'DataSource')
    cursor = conn.cursor() # helps to execute the query and fetch the records from the database.

    # connection dwh
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.psql_conn(conf_dwh, 'DataWarehouse')
    cursor_dwh = conn_dwh.cursor()
    
    # get query string
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(path_query+'query.sql', 'r').read(), strip_comments=True
    ).strip() # stripping comments

    # get schema dwh_design
    path_dwh_design = os.getcwd()+'/query/'
    query_dwh = sqlparse.format(
        open(path_dwh_design+'query_dwh.sql', 'r').read(), strip_comments=True
    ).strip() # stripping comments

    try:
        # get & print data
        print("[INFO] Service-ETL is Running")
        df = pd.read_sql(query, engine)

        # create schema dwh
        cursor_dwh.execute(query_dwh)
        conn_dwh.commit()

        # ingest data to postgresql database
        df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False) # mode append atau nambahin ke nama table 'dim_orders'
        print("[INFO] Service-ETL is Successful")
    except Exception as e:
        print("[INFO] Service-ETL is Failed")
        print(str(e))


