# I write and test methods one at a time.
# This file contains unit tests of individual methods.
import json

from src import SQLHelper, RDBDataTable
import pymysql
from src.RDBDataTable import RDBDataTable
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


def find_by_primary_key_test():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.people"

    rdb_table = RDBDataTable(table_name, ['playerID'], connection)

    result = rdb_table.find_by_primary_key(['aardsda01'], ['playerID', 'birthYear', 'birthMonth', 'birthDay'])

    if result[1] is not None:
        print(json.dumps(result[1], indent=2))
    else:
        print("None.")


find_by_primary_key_test()


def find_by_template_test():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.people"

    rdb_table = RDBDataTable(table_name, ['playerID'], connection)

    template = {"birthCity": "La Guaira"}

    result = rdb_table.find_by_template(template, ['playerID', 'birthYear', 'birthMonth', 'birthDay'])

    if result[1] is not None:
        print(json.dumps(result, indent=2))
    else:
        print("None.")


find_by_template_test()


def delete_by_key_test():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.people"

    rdb_table = RDBDataTable(table_name, ['playerID'], connection)

    result = rdb_table.delete_by_key(['abbotgl01'])

    if result[0] is not None:
        print("%d rows deleted" % result[0])
    else:
        print("None.")


delete_by_key_test()


def delete_by_template_test():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.people"

    rdb_table = RDBDataTable(table_name, ['playerID'], connection)

    template = {"birthCity": "Zanesville"}

    result = rdb_table.delete_by_template(template)

    if result[0] is not None:
        print("%d rows deleted" % result[0])
    else:
        print("None.")


delete_by_template_test()


def update_by_key_test():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.batting"

    rdb_table = RDBDataTable(table_name, ['playerID', 'yearID', 'stint', 'teamID'], connection)

    result = rdb_table.update_by_key(["aardsda01", "2010", "1", "SEA"], {"teamID": "ATL", "AB": "11"})

    if result[1] is not None:
        print("%d rows updated" % result[0])
        print(json.dumps(result[0], indent=2))
    else:
        print("None.")


update_by_key_test()


def update_by_template():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.batting"

    rdb_table = RDBDataTable(table_name, ['playerID', 'yearID', 'stint', 'teamID'], connection)

    template = {"yearID": "1956"}

    result = rdb_table.update_by_template(template, {"teamID": "ATL", "AB": "11"})

    if result[0] is not None:
        print("%d rows updated" % result[0])
    else:
        print("None.")


update_by_template()


def insert():

    connection = SQLHelper.get_default_connection()

    table_name = "lahman2019raw.batting"

    rdb_table = RDBDataTable(table_name, ['playerID', 'yearID', 'stint', 'teamID'], connection)

    result = rdb_table.insert({"playerID": "test1", "yearID": "test", "stint": 1, "teamID": "test"})

    if result[0] is not None:
        print("%d rows inserted" % result[0])
    else:
        print("None.")


insert()

