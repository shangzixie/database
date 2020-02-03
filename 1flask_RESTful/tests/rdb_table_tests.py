import json
from src.RDBDataTable import RDBDataTable
import logging
import pymysql
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def init_data_table_test(table_name=None, connect_info=None, key_columns=["playerID"]):
    """
    :param table_name:
    :param connect_info:
    :param key_columns:
    :return:
    """

    # test case
    table_name = "People"
    connect_info = {"host": 'localhost',
                    "user": 'dbuser',
                    "password": 'lichiqu123',
                    "db": 'lahman2019raw',
                    "charset": 'utf8mb4',
                    "cursorclass": pymysql.cursors.DictCursor}

    rdb_data_table = RDBDataTable(table_name=table_name, connect_info=connect_info, key_columns=key_columns)
    return rdb_data_table

def test_run():

    datatable = init_data_table_test()
    sql = "select * from hw1_schema.people where nameLast=%s;"
    args = ('Williams')

    result = datatable.run_q(sql=sql, args=args, fetch = True)

    print("Return code = ", result[0])
    print("Data = ")
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")

def find_by_primary_key_test():
    """
    :param data_table:
    :param key_fields:
    :param field_list:
    :return:
    """
    # test case
    datatable = init_data_table_test()
    key_fields = ['anthoer01']
    field_list = ['nameLast', 'nameFirst', 'birthYear', 'birthState', 'birthMonth']

    sql, args = datatable.find_by_primary_key(key_fields, field_list)
    print("SQL = ", sql, ", args = ", args)

    result = datatable.run_q(sql, args)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")

def find_by_template_test():
    datatable = init_data_table_test()
    # template = {"birthYear": "1917"}
    # field_list = ["williac01"]
    # res = datatable.find_by_template(template, field_list=field_list)
    # print(res)
    table_name = "hw1_schema.people"
    fields = ['nameLast', 'nameFirst', 'birthYear', 'birthState', 'birthMonth','playerID']
    template = {"nameLast": "Anderson", "birthState": "CA", "birthMonth": "9"}
    sql, args = datatable.find_by_template(template, fields)
    print("SQL = ", sql, ", args = ", args)

    result = datatable.run_q(sql, args)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


def delete_by_key_test():
    """
    :param data_table:
    :param key_fields:
    :return:
    """
    # test case
    datatable = init_data_table_test()
    result = datatable.delete_by_key(['billibr01'])
    print(result)
    print(datatable.find_by_primary_key(['billibr01']))

def delete_by_template_test():
    datatable = init_data_table_test()
    template = {"nameFirst": "Bill", "birthState": "OH"}
    result = datatable.delete_by_template(template)
    print(result)
    print(datatable.find_by_template(template))

def update_by_template_test():
    datatable = init_data_table_test()
    new_cols = {
        "birthState": "NY",
        "birthCity": "NewYork",
    }

    template = {"birthYear": "1862", "deathYear": "1930"}

    result = datatable.update_by_template(template, new_cols)
    print(result)
    fields = ['nameLast', 'nameFirst', 'birthYear', 'birthState', 'birthMonth', 'playerID','birthCity','birthState']
    sql, args = datatable.find_by_template(template, fields)
    print("SQL = ", sql, ", args = ", args)

    result = datatable.run_q(sql, args)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


def update_by_key_test():
    datatable = init_data_table_test()
    new_cols = {
        "birthState": "OH",
        "birthCity": "Portage",
    }

    key_field = ['abregjo01']

    result = datatable.update_by_key(key_field, new_cols)
    print(result)

def insert_test():
    datatable = init_data_table_test()
    row = {
        "playerID": "dissjdj01",
        "birthYear": "1990",
        "birthMonth": "6",
        "birthDay": "12",
        "birthCountry": "USA",
        "birthState": "OH",
        "birthCity": "Hamilton",
        "deathYear": "1996",
        "deathMonth": "8",
        "deathDay": "4",
        "deathCountry": "USA",
        "deathState": "NY",
        "deathCity": "Chicago",
        "nameFirst": "Harry",
        "nameLast": "Fin",
        "weight": "200",
        "height": "70",
        "bats": "R",
        "throws": "R",
        "debut": "1998-10-22",
        "finalGame": "1996-01-03",
        "retroID": "djsux01",
        "bbrefID": "djsux01",
    }

    datatable.insert(row)
    template = {"playerID": "dissjdj01"}
    fields = ['nameLast', 'nameFirst', 'birthYear', 'birthState', 'birthMonth', 'playerID','birthCity','birthState']
    sql, args = datatable.find_by_template(template, fields)
    print("SQL = ", sql, ", args = ", args)

    result = datatable.run_q(sql, args)
    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")




#test_run()
#find_by_primary_key_test()
#delete_by_key_test()
#find_by_template_test()
#delete_by_template_test()
#update_by_template_test()
#update_by_key_test()
insert_test()