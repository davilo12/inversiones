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

query_loans = "SELECT * FROM Prestamos"
df_loans = sql_conn.read_data(query_loans)
df_loans['bedoya'] = df_loans['bedoya'].astype(int)
df_loans['chaverra'] = df_loans['chaverra'].astype(int)
df_loans['villafane'] = df_loans['villafane'].astype(int)

now = datetime.date.today()
payment_details = {
	"mortgage": ["1622"],
	"id": ["123"],
	"payment_date": [now],
	"amount": [640000]
}
df_payment = pd.DataFrame.from_dict(payment_details)
df_payment['amount'] = df_payment['amount'].astype(int)

inner = pd.merge(df_loans, df_payment, how="inner", on=["mortgage", "id"])
inner["bedoya"] = (inner["bedoya"]/inner["total"])*inner["amount"]
inner["chaverra"] = (inner["chaverra"]/inner["total"])*inner["amount"]
inner["villafane"] = (inner["villafane"]/inner["total"])*inner["amount"]

final = inner[["mortgage", "id", "payment_date", "amount", "bedoya", "chaverra", "villafane"]]
sql_conn.run_query(df=final, table="Payments", operation_type="INSERT")

sql_conn.close_conn()
