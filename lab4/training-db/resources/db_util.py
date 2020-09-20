import os

import pandas as pd
import sqlalchemy as db
from sqlalchemy import Column, Float, Table, Integer
from sqlalchemy.ext.declarative import declarative_base


class DBUtil:

    def __init__(self):
        db_loc = os.environ['DB_LOC']
        self.engine = db.create_engine('sqlite:///' + db_loc + 'trainingdata.db', echo=True)
        self.Base = declarative_base()
        self.Base.metadata.reflect(self.engine)

    def create_tb(self, table_name, column_names):
        conn = self.engine.connect()
        trans = conn.begin()
        columns = (Column(name, Float, quote=False) for name in column_names)
        v_table = Table(table_name, self.Base.metadata, Column('id', Integer, primary_key=True, autoincrement=True),
                        extend_existing=True, *columns)
        v_table.create(self.engine, checkfirst=True)
        trans.commit()

    def drop_tb(self, table_name):
        conn = self.engine.connect()
        trans = conn.begin()
        v_table = self.Base.metadata.tables[table_name]
        v_table.drop(self.engine, checkfirst=True)
        trans.commit()

    def add_data_records(self, table_name, records):
        v_table = self.Base.metadata.tables[table_name]
        query = db.insert(v_table)
        connection = self.engine.connect()
        trans = connection.begin()
        connection.execute(query, records)
        trans.commit()

    def read_data_records(self, table_name):
        v_table = self.Base.metadata.tables[table_name]
        connection = self.engine.connect()
        trans = connection.begin()
        query = db.select([v_table])
        df = pd.read_sql_query(query, con=connection)
        trans.commit()
        return df
