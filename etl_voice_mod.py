import psycopg2
import pandas as pd

# Database Setup

#Postgresql
my_sql_password = "INSERT HERE YOUR POSTGRES PASSWORD"
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

###############     Treating RegUsers to insert in database - Pandas
print('Starting to work on Regusers')

df_users = pd.read_csv('regusers.tsv', sep='\t', engine='python')
df_users.fillna('Null', inplace=True)
df_users.drop(df_users.tail(2).index, inplace=True)
users_list = df_users.values.tolist()
create_tbl_users = "CREATE TABLE IF NOT EXISTS regusers( " \
                "SWID VARCHAR(250) NOT NULL," \
                "BIRTH_DT VARCHAR(25)," \
                "GENDER_CD VARCHAR(25)," \
                "PRIMARY KEY (SWID));"

cur.execute(create_tbl_users, users_list)
conn.commit()
cur.execute("SELECT * FROM regusers ORDER BY SWID DESC LIMIT 1;")

try:
    regusers_last_value_inserted = list(cur.fetchone())
except:
    regusers_last_value_inserted = list()

if not regusers_last_value_inserted:
    print(regusers_last_value_inserted)
    print(type(regusers_last_value_inserted))
    copy_query = "INSERT INTO regusers (SWID,BIRTH_DT,GENDER_CD) VALUES (%s,%s,%s);"
    cur.executemany(copy_query, users_list)
    conn.commit()
    print('first data ever')

if regusers_last_value_inserted == users_list[-1]:
    print('all good, data already in table regusers \nnothing to add')
else:
    print(regusers_last_value_inserted)
    copy_query = "INSERT INTO regusers (SWID,BIRTH_DT,GENDER_CD) VALUES (%s,%s,%s);"
    cur.executemany(copy_query, users_list)
    conn.commit()
    print('new records to append to NoSql_db')


###############   Treating UrlMap to insert in database - Postegresql COPY command
print('Starting to work on url_map')

create_tbl_url_map = "CREATE TABLE IF NOT EXISTS url_map(" \
                    "url VARCHAR(250)," \
                    "category VARCHAR(50));"

cur.execute(create_tbl_url_map)

url_map_file = list()
with open('urlmap.tsv', 'r') as um:
    for line in um:
        url_map_file.append(line.rstrip('\n').split('\t'))

# Check last value inserted in table
cur.execute("SELECT * FROM url_map ORDER BY url DESC LIMIT 1;")
url_map_last_value_inserted = cur.fetchone()

if url_map_last_value_inserted is None:
    file = open('urlmap.tsv', 'r')
    to_db = cur.copy_from(file, 'url_map', sep='\t', null='')
    delete_headers = "DELETE FROM url_map WHERE url= 'url' AND category = 'category'"
    cur.execute(delete_headers)
    conn.commit()
    print('first data ever')

elif tuple(url_map_file[-1]) == url_map_last_value_inserted:
    print('all good, data already in table url_map')
    print('nothing to add')

else:
    truncate_table = "TRUNCATE TABLE url_map;"
    cur.execute(truncate_table)
    conn.commit()
    file = open('urlmap.tsv', 'r')
    to_db = cur.copy_from(file, 'url_map', sep='\t', null='')
    delete_headers = "DELETE FROM url_map WHERE url= 'url' AND category = 'category'"
    cur.execute(delete_headers)
    conn.commit()
    print('new records to append to NoSql_db')


###############     Treating WebLog to insert in database - Pure python to clean data
print('Starting to work on web_log')
create_tbl_wl = "CREATE TABLE IF NOT EXISTS web_log(" \
                    "id INT," \
                    "date_time VARCHAR(50)," \
                    "col_3 bigint," \
                    "col_4 bigint," \
                    "col_5 VARCHAR(50)," \
                    "col_6 VARCHAR(50)," \
                    "col_7 INT," \
                    "IP VARCHAR(50)," \
                    "col_9 INT," \
                    "col_10 float," \
                    "col_11 INT," \
                    "link_url VARCHAR(250)," \
                    "device_id VARCHAR(250)," \
                    "col_14 VARCHAR(50)," \
                    "col_15 VARCHAR(50)," \
                    "col_16 INT, " \
                    "col_17 INT," \
                    "col_18 INT," \
                    "col_19 VARCHAR(50), " \
                    "col_20 VARCHAR(50)," \
                    "col_21 VARCHAR(50), " \
                    "col_22 INT, " \
                    "col_23 float," \
                    "col_24 INT," \
                    "col_25 VARCHAR(50)," \
                    "col_26 VARCHAR(50)," \
                    "col_27 INT, " \
                    "col_28 INT," \
                    "col_29 VARCHAR(50)," \
                    "used_system VARCHAR(500)," \
                    "col_31 INT, col_32 INT, col_33 INT, col_34 INT," \
                    "col_35 float," \
                    "city VARCHAR(50)," \
                    "country VARCHAR(50)," \
                    "col_38 INT," \
                    "state_code VARCHAR(50)," \
                    "col_40 float," \
                    "col_41 float," \
                    "col_42 float," \
                    "col_43 float," \
                    "col_44 float," \
                    "col_45 VARCHAR(50), " \
                    "col_46 float," \
                    "col_47 VARCHAR(50)," \
                    "col_48 VARCHAR(50)," \
                    "col_49 float);"

cur.execute(create_tbl_wl)
conn.commit()
cur.execute("SELECT * FROM web_log LIMIT 1;")
web_log_first_value_inserted = cur.fetchone()
web_log_list = []

with open('web_log.tsv', 'r') as wb:
    for line in wb:
        web_log_list.append(line.rstrip('\n'))

# Find null columns indexes
null_columns = []

for idx, column in enumerate(web_log_list[0].split('\t')):
    if column.startswith('Unnamed', 0, 7):
        null_columns.append(idx)

new_web_log_list = []
for row in web_log_list:
    new_web_log_list.append(row.split('\t'))

reversed_null_columns = null_columns[::-1]

# Delete the null columns
for row in new_web_log_list:
    for index in reversed_null_columns:
        del row[index]

for row in new_web_log_list:
    del row[0]
    for position, column in enumerate(row):
        if column == '':
            row[position] = 'Null'
        if column.startswith('{') & column.endswith('}'):
            row[position] = column.replace('{', '').replace('}', '')
        else:
            None

if web_log_first_value_inserted is None:
    insert_query = "INSERT INTO web_log (id, date_time, col_3, col_4, col_5, col_6, " \
                   "col_7, IP, col_9, col_10, col_11, link_url, device_id, col_14, col_15, col_16," \
                   "col_17, col_18, col_19, col_20, col_21, col_22, col_23, col_24, col_25, col_26, col_27, " \
                   "col_28, col_29, used_system, col_31, col_32, col_33, col_34, col_35, city, country, col_38," \
                   "state_code, col_40 , col_41 , col_42, col_43, col_44, col_45, col_46," \
                   "col_47, col_48,col_49)" \
                   " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                   "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    cur.executemany(insert_query, new_web_log_list)
    conn.commit()
    print('first data ever')

elif web_log_first_value_inserted[1] == (web_log_list[0].split('\t'))[2]:
    print('all good, data already in table web_log\nnothing to add')
else:
    insert_query = "INSERT INTO web_log (id, date_time, col_3, col_4, col_5, col_6, " \
                   "col_7, IP, col_9, col_10, col_11, link_url, device_id, col_14, col_15, col_16," \
                   "col_17, col_18, col_19, col_20, col_21, col_22, col_23, col_24, col_25, col_26, col_27, " \
                   "col_28, col_29, used_system, col_31, col_32, col_33, col_34, col_35, city, country, col_38," \
                   "state_code, col_40 , col_41 , col_42, col_43, col_44, col_45, col_46," \
                   "col_47, col_48,col_49)" \
                   " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"\
                   "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

    cur.executemany(insert_query, new_web_log_list)
    conn.commit()
    print('new records to append to NoSql_db')

conn.close()
