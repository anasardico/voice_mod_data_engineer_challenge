from psycopg2.extras import RealDictCursor
import pymongo
import psycopg2

#MongoDb
Mongodb_Client = pymongo.MongoClient("mongodb://localhost:27017/")
database = Mongodb_Client["Voice_Mod_db"]
aggr_table_mongo = database["Analytics_table"]

#Postgresql
my_sql_password = "INSERT PASSWORD HERE"
conn = None
print('Connecting to the PostgreSQL database...')
try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        port="5432",
        password=my_sql_password
    )
    print('Connection succeed')
except: print('Connection Error')

cur = conn.cursor()

## INSERT OR APPEND TO MONGODB:

aggregate_table = "SELECT " \
                  "ru.swid AS user_id," \
                  "wl.id AS web_id, " \
                  "wl.date_time, " \
                  "wl.country," \
                  "wl.city, " \
                  "ru.birth_dt, " \
                  "ru.gender_cd," \
                  "wl.state_code," \
                  "um.category," \
                  "um.url AS product_link " \
                  "FROM web_log AS wl " \
                  "LEFT JOIN url_map AS um ON wl.link_url=um.url " \
                  "LEFT JOIN regusers AS ru ON wl.device_id=ru.swid ;"

cur = conn.cursor(cursor_factory=RealDictCursor)
cur.execute(aggregate_table)
conn.commit()
data_dict = [dict(row) for row in cur.fetchall()]

last_insert_mongo = list(aggr_table_mongo.find().sort("_id", pymongo.DESCENDING).limit(1))

if not last_insert_mongo:
    last_insertion_date = list()
else:
    last_insertion_date = last_insert_mongo[0].get("date_time")

data_nosql = data_dict[-1].get("date_time")

if not last_insert_mongo:
    aggr_table_mongo.insert_many(data_dict)
    print('Data added with success')

if last_insertion_date == data_nosql:
    print('all data already in mongo table')

else:
    new_date = "SELECT " \
               "ru.swid AS user_id," \
               "wl.id AS web_id, " \
               "wl.date_time, " \
               "wl.country," \
               "wl.city, " \
               "ru.birth_dt, " \
               "ru.gender_cd," \
               "wl.state_code," \
               "um.category," \
               "um.url AS product_link " \
               "FROM web_log AS wl " \
               "LEFT JOIN url_map AS um ON wl.link_url=um.url " \
               "LEFT JOIN regusers AS ru ON wl.device_id=ru.swid " \
               "WHERE wl.date_time::DATE = CURRENT_DATE;"

    cur.execute(new_date)
    conn.commit()
    aggr_table_mongo.insert_many(data_dict)
    print('New daily Data added with success')
