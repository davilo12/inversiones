import pyodbc
import pandas as pd


class SQL:
	def __init__(self, driver, server, database, username, password):
		self.driver = driver
		self.server = server
		self.database = database
		self.username = username
		self.password = password
		self.conn = None

	def connection(self):
		self.conn = pyodbc.connect(
			f"DRIVER={self.driver};"
			f"SERVER=tcp:{self.server};"
			f"PORT=1433;"
			f"DATABASE={self.database};"
			f"UID={self.username};"
			f"PWD={self.password}"
		)
		return self.conn

	def run_query(self, df, table, operation_type='INSERT'):
		if operation_type == 'SELECT':
			self.select_data(df, table)
		elif operation_type == 'INSERT':
			self.insert_data(df, table)
		elif operation_type == 'MERGE':
			self.merge_data(df, table)

		self.conn.commit()

	def select_data(self, df, table):
		cursor = self.conn.cursor
		cols = ', '.join(df.columns)
		cursor.execute(f"SELECT {cols} FROM {table}")
		rows = cursor.fetchall()
		column_names = [desc[0] for desc in cursor.description]
		df = pd.DataFrame([tuple(row) for row in rows], columns=column_names)
		return df

	def insert_data(self, df, table):
		cursor = self.conn.cursor()
		cols = ', '.join(df.columns)
		for _, row in df.iterrows():
			placeholders = ', '.join(['?' for _ in range(len(row))])
			cursor.execute(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", tuple(row))

	def merge_data(self, df, table):
		# Implement MERGE logic as needed
		# Example: MERGE INTO TargetTable USING SourceTable ON condition WHEN MATCHED THEN ... WHEN NOT MATCHED THEN ...
		pass

	def read_data(self, string):
		cursor = self.conn.cursor()
		cursor.execute(string)
		rows = cursor.fetchall()
		column_names = [desc[0] for desc in cursor.description]
		df = pd.DataFrame([tuple(row) for row in rows], columns=column_names)

		return df

	def close_conn(self):
		self.conn.cursor().close()
