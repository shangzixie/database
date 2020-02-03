# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")

def test_init():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("People", connect_info, key_columns=['playerID'])
    print("loaded table= \n", csv_tbl)

def test_match():
    row = {'cool': 'yes', 'db':'no'}
    t = {'cool': '1'}
    result = CSVDataTable.matches_template(row, t)
    print(result)


def test_find_by_template():
    tem = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("People", connect_info, key_columns=['playerID'])
    result = csv_tbl.find_by_template(tem)
    print(result)

def test_find_by_primarykey():
    pkey = ['aasedo01']
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("People", connect_info, key_columns=['playerID'])
    result1 = csv_tbl.find_by_primary_key(key_fields=pkey)
    print(result1)



def test_key_to_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("People", connect_info, key_columns=['playerID'])
    k = csv_tbl.key_to_template(['willite01'])
    print(k)

def test_delete_By_Key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("People", connect_info, key_columns=['birthDay'])
    result = csv_tbl.delete_by_key(['8'])
    print(result)

def test_delelt_By_Tmp():
    tem = {'nameLast': 'Williams', 'birthState': 'NC'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("People", connect_info, None)
    result = csv_tbl.delete_by_template(tem)
    print(result)
    print(csv_tbl.find_by_template({'nameLast': 'Williams', 'birthState': 'NC'}))

def test_update_by_tmp():
    tem = {'nameLast': 'Williams', 'nameFirst': 'Ted'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    new_value = {
        'birthCity': 'Orange'
    }
    csv_tbl = CSVDataTable("People", connect_info, None)
    result = csv_tbl.update_by_template(tem, new_value)
    print(result)
    print(csv_tbl.find_by_template({'nameLast': 'Williams', 'nameFirst': 'Ted'}))

def test_insertrecord():
    new = {'nameLast': 'Sara', 'nameFirst': 'Ted'}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }
    csv_tbl = CSVDataTable("People", connect_info, None)
    result = csv_tbl.insert(new)
    print(result)
    print(csv_tbl.find_by_template({'nameLast': 'Sara', 'nameFirst': 'Ted'}))


#test_find_by_template()
#test_find_by_primarykey()
#test_delete_By_Key()
#test_delelt_By_Tmp()
#test_update_by_tmp()
test_insertrecord()