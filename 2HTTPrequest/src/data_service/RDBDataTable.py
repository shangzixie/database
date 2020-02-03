

#########################################################
#
#
# YOU HAVE TO IMPLEMENT SOME METHODS FROM THIS FILE, BUT MOST OF THE FILE
# IS JUST THE SOLUTION TO THE HW1 RDBDataTable SECTION.
#
# LOOK FOR --TO IMPLEMENT-- TO FIND THE FUNCTIONS/METHODS YOU NEED TO IMPLEMENT.
#
# ALSO, SEE THINGS OF THE FORM -- THANK ALY AND ARA --
#
#########################################################

# Pandas is only used to pretty print the table contents.
import pandas as pd
import pymysql
import json

# The helper functions from homework 1 are in this file. The RDBDataTable code below calls the functions.
import src.data_service.dbutils as dbutils

# You do not need to do anything about the logging stuff. You can just ignore, or you can use if you want.
import logging
logger = logging.getLogger()

# Makes pretty print the RDBDataTable rows a little better.
pd.set_option('display.width', 256)
pd.set_option('display.max_columns', 12)


class RDBDataTable():
    """
    RDBDataTable is relation DB implementation of the BaseDataTable.
    I have removed the dependency/subclassing from BaseDataTable to reduce confusion.
    """

    # Default connection information in case the code does not pass an object
    # specific connection on object creation.
    #
    # NOTE: You may just use the default connector if you want.
    _default_connect_info = {
        'host': 'localhost',
        'user': 'dbuser',
        'password': 'dbuserdbuser',
        'db': 'lahman2019clean',
        'port': 3306
    }

    _rows_to_print = 5

    def __init__(self, table_name, db_name, key_columns=None, connect_info=None, debug=True):
        """

        :param table_name: The name of the RDB table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        # RDBDataTable is not told the keys. It can extract from the schema using DML statememts.
        if key_columns is not None:
            raise ValueError("RDBs know the keys. You should set in the DB use DML."
            )

        # Initialize and store information in the parent class.
        super().__init__()

        # If there is not explicit connect information, use the defaults.
        # You can use the default.
        if connect_info is None:
            self._connect_info = RDBDataTable._default_connect_info

        # Create a connection to use inside this object. In general, this is not the right approach.
        # There would be a connection pool shared across many classes and applications.
        self._cnx = pymysql.connect(
            host=self._connect_info['host'],
            user=self._connect_info['user'],
            password=self._connect_info['password'],
            db=self._connect_info['db'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

        if db_name is None or table_name is None:
            raise ValueError("You MUST pass a database name and table name.")

        self._db_name = db_name
        self._table_name = table_name
        self._full_table_name = db_name + "." + table_name

        self._connect_info = connect_info
        self._row_count = None
        self._key_columns = None
        self._sample_rows = None
        self._related_resources = None
        self_columns = None


        """
        You should implement these methods. See the implementation templates below.
        """
        self.get_primary_key_columns()
        self.get_row_count()
        self.get_sample_rows()
        self.get_related_resources()

    def __str__(self):
        """

        :return: String representation of the data table.
        """
        result = "RDBDataTable: "
        result += "\ntable_name = " + self._table_name
        result += "\ndb_name = " + self._db_name
        result += "\nTable type = " + str(type(self))
        result += "\nKey fields: " + str(self._key_columns)
        result += "\nNo. of rows = " + str(self._row_count)
        result += "\nA few sample rows = \n" + str(self._sample_rows)
        result += "\nRelated resources:\n" + json.dumps(self._related_resources, indent=2)

        return result

    def get_row_count(self):


        """

        :return: Returns the count of the number of rows in the table.
        """
        q = "select count(*) from " + self._full_table_name
        result, data = dbutils.run_q(q, conn=self._cnx)
        return data

        # -- TO IMPLEMENT --

    def get_primary_key_columns(self):
        """

        :return: A list of the primary key columns ordered by their position in the key.
        """

        # -- TO IMPLEMENT --
        q = "SHOW KEYS FROM " + self._full_table_name + " WHERE Key_name = 'PRIMARY'"
        self._key_columns = pd.read_sql(q, self._cnx)['Column_name'].tolist()
        #print(self._key_columns)

        # Hint. Google "get primary key columns mysql"
        # Hint. THE ORDER OF THE COLUMNS IN THE KEY DEFINITION MATTERS.

    def get_sample_rows(self, no_of_rows=_rows_to_print):
        """

        :param no_of_rows: Number of rows to include in a sample of the data.
        :return: A Pandas dataframe containing the first _row_to_print number of rows.
        """
        q = "select * from " + self._full_table_name + " limit " + str(RDBDataTable._rows_to_print)
        self._sample_rows = pd.read_sql(q, self._cnx)

    def get_related_resources(self):
        """

        :return:
        """

        # -- THANK ALY AND ARA --
        # Aly and Ara told me to get rid of this requirement or they would be unhappy.
        #
        pass

    def get_links(self, target_table = None):


        # -- THANK ALY AND ARA --
        # Aly and Ara told me to get rid of this requirement or they would be unhappy.
        #
        pass

    ####################################################################################################
    #
    # THE METHODS BELOW ARE BASICALLY THE SOLUTION TO THE RDB PART OF HW1
    #
    #
    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the request fields for the record identified
            by the key.
        """

        # Get the key_columns specified on table create.
        # Later on, we will learn how to get the information from the schema in the DB.
        key_columns = self._key_columns

        # Zipping together key_columns and passed fields produces a valid template
        tmp = dict(zip(key_columns, key_fields))

        # Call find_by_template.
        result = self.find_by_template(tmp, field_list)

        if result is not None and len(result) > 0:
            result = result[0]
        else:
            result = None

        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None, commit=True):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        result = None

        try:
            sql, args = dbutils.create_select(self._full_table_name, template=template, fields=field_list)
            res, data = dbutils.run_q(sql=sql, args=args, conn=self._cnx, commit=True, fetch=True)
        except Exception as e:
            print("Exception e = ", e)
            raise e

        return list(data)

    def delete_by_template(self, template):
        """

        Deletes all records that match the template.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        try:
            sql, args = dbutils.create_select(self._full_table_name, template=template, is_select=False)
            res, d = dbutils.run_q(sql, args=args, conn=self._cnx, commit=True)
            return res
        except Exception as e:
            print("Got exception e = ", e)
            raise e

    def delete_by_key(self, key_fields):

        # Get the key_columns specified on table create.
        # Later on, we will learn how to get the information from the schema in the DB.
        key_columns = self._key_columns

        # Zipping together key_columns and passed fields produces a valid template
        tmp = dict(zip(key_columns, key_fields))

        # Call find_by_template.
        result = self.delete_by_template(tmp)
        return result

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        # Get the list of columns.
        sql, args = dbutils.create_insert(self._full_table_name, new_record)
        res, d = dbutils.run_q(sql, args=args, conn=self._cnx)
        return res

    def update_by_template(self, template, new_values):
        """

        :param template: A template that defines which matching rows to update.
        :param new_values: A dictionary containing fields and the values to set for the corresponding fields
            in the records.
        :return: The number of rows updates.
        """
        sql, args = dbutils.create_update(self._full_table_name, template=template, changed_cols=new_values)
        res, d = dbutils.run_q(sql, args=args, conn=self._cnx, commit=True)
        return res

    def update_by_key(self, key_fields, new_values):

        # Get the key_columns specified on table create.
        # Later on, we will learn how to get the information from the schema in the DB.
        key_columns = self._key_columns

        # Zipping together key_columns and passed fields produces a valid template
        tmp = dict(zip(key_columns, key_fields))

        # Update
        res = self.update_by_template(template=tmp, new_values=new_values)
        return res

    ####################################################################################################
    #
    # THE METHODS BELOW WERE GOING TO BE PART OF HW2, BUT ...
    #
    #
    # -- THANK ALY AND ARA --
    #
    #
    pass

    def _get_key_map(self, target_name):
        # -- THANK ALY AND ARA --
        # Aly and Ara told me to get rid of this requirement or they would be unhappy.
        #
        pass


    def navigate_path(self, pk, target_name, query_template, fields):
        # -- THANK ALY AND ARA --
        # Aly and Ara told me to get rid of this requirement or they would be unhappy.
        #
        pass


    def navigate_path_and_key(self, pk, target_name, tk, fields):
        # -- THANK ALY AND ARA --
        # Aly and Ara told me to get rid of this requirement or they would be unhappy.
        #
        pass

