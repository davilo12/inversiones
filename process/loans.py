import os
import datetime
import pandas as pd
from library.sql import SQL


sql_conn = SQL(
	driver="{ODBC Driver 18 for SQL Server}",
	server=os.environ['SERVER'],
	database=os.environ['DATABASE'],
	username=os.environ['USERNAME'],
	password=os.environ['PASSWORD']
)
sql_conn.connection()

now = datetime.date.today()
loan_details = {
	"mortgage": ["2652"],
	"debtor": "Gabriel Sanchez",
	"id": "456",
	"loan_date": now,
	"cellphone": "3104267471",
	"bedoya": [15000000],
	"chaverra": [1000000],
	"villafane": [1000000]
}
df_payment = pd.DataFrame.from_dict(loan_details)
sql_conn.run_query(df=df_payment, table="Prestamos", operation_type='INSERT')

# Just to verify
select_query = "SELECT * FROM Prestamos"
df_check = sql_conn.read_data(select_query)

sql_conn.close_conn()
