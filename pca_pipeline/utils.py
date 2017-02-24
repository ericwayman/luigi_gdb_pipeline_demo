import re
import psycopg2 as pg
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import register_adapter
import os

class NotSqlIdentifierError(Exception):
    pass

valid_pattern = r'^[a-zA-Z0-9_\.\$]*$'

class QuotedIdentifier(object):
    def __init__(self, obj_str):
        self.obj_str = obj_str

    def getquoted(self):
        if re.match(valid_pattern, self.obj_str):
            return self.obj_str
        else:
            raise NotSqlIdentifierError(repr(self.obj_str))

register_adapter(QuotedIdentifier, lambda x: x)

class PSQLConn(object):
    """Stores the connection to psql."""
    def __init__(self, db, user, password, host,port=5432):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        connection = pg.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password,
                port = self.port)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection


#helper function to open functions.sql file
src_path = os.path.dirname(os.path.abspath(__file__))
sql_path = os.path.join(src_path,'sql/')

def initialize_user_defined_functions():
    user_defined_functions_file = os.path.join(sql_path,'functions.sql')
    with open(user_defined_functions_file) as f:
        cmd = f.read()
    return cmd



def create_num_flows_for_feature_table(conn,input_table, output_table, user_column):
    with conn.cursor() as curs:
        query = curs.mogrify("""
        SELECT create_num_flows_for_feature_table('%s','%s','%s')
        """,(QuotedIdentifier(input_table),QuotedIdentifier(output_table),QuotedIdentifier(user_column)))
        curs.execute(query)

def create_pca_input_table(conn,input_table, output_table,user_column,id):
    with conn.cursor() as curs:
        query = curs.mogrify("""
            SELECT create_pca_input_table('%s','%s','%s',%s)
            """,(QuotedIdentifier(input_table),
                QuotedIdentifier(output_table),
                QuotedIdentifier(user_column),
                id
            )
        )
        curs.execute(query)

def find_principal_components(conn,input_table,output_table,time_id_col,name_id_col,val_col,percentage_val):
    with conn.cursor() as curs:
        query = curs.mogrify("""
            SELECT  find_principal_components('%s','%s','%s','%s','%s',%s)
        """,(QuotedIdentifier(input_table),
            QuotedIdentifier(output_table),
            QuotedIdentifier(time_id_col),
            QuotedIdentifier(name_id_col),
            QuotedIdentifier(val_col),
            percentage_val
            )
        )
        curs.execute(query)

def extract_large_pca_components(conn,output_table,base_feature_table,pca_table,user_column,threshold):
    with conn.cursor() as curs:
        query=curs.mogrify("""
            SELECT extract_large_pca_components('%s','%s','%s','%s',%s)
            """,(QuotedIdentifier(output_table),
                QuotedIdentifier(base_feature_table),
                QuotedIdentifier(pca_table),
                QuotedIdentifier(user_column),
                threshold
                )
        )
        curs.execute(query)
