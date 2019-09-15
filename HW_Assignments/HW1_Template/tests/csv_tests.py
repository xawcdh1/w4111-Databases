# I write and test methods one at a time.
# This file contains unit tests of individual methods.
import json

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


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "Batting.csv"
    }

    csv_tbl = CSVDataTable("batting", connect_info, ['playerID', 'yearID', 'stint', 'teamID'])

    print("Created table = " + str(csv_tbl))

    return csv_tbl


def find_by_primary_key_test():

    csv_tbl = t_load()

    key_fields = ['allisar01', '1871', '1', 'CL1']

    print("find by primary key\n" + json.dumps(key_fields))

    res = csv_tbl.find_by_primary_key(key_fields, ['playerID', 'G', 'AB', 'R'])

    if res is not None:
        print(json.dumps(res, indent=2))
    else:
        print("None.")


# find_by_primary_key_test()


def find_by_template_test():

    csv_tbl = t_load()

    template = {"yearID": "1871"}

    print("find by template\n" + json.dumps(template, indent=2))

    res = csv_tbl.find_by_template(template, ['playerID', 'G', 'AB', 'R'])

    if res is not None:
        print(json.dumps(res, indent=2))

    else:
        print("None.")


# find_by_template_test()


def delete_by_key_test():

    csv_tbl = t_load()

    key_fields = ['abercda01', '1871', '1', 'TRO']

    print("delete by key\n" + json.dumps(key_fields))

    result = csv_tbl.delete_by_key(key_fields)

    if result is not None:
        print("%d rows deleted" % result)
    else:
        print("None.")


delete_by_key_test()


def delete_by_template_test():

    csv_tbl = t_load()

    template = {"G": "21"}

    print("delete by template\n" + json.dumps(template, indent=2))

    result = csv_tbl.delete_by_template(template)

    if result is not None:
        print("%d rows deleted" % result)
    else:
        print("None.")


# delete_by_template_test()


def update_by_key_test():

    csv_tbl = t_load()

    new_values = {"teamID": "ATL", "AB": "11"}

    print("update by key\n" + json.dumps(new_values, indent=2))

    result = csv_tbl.update_by_key(["addybo01", "1871", "1", "RC1"], new_values)

    if result is not None:
        print("%d rows updated" % result)
    else:
        print("None.")


# update_by_key_test()


def update_by_template():

    csv_tbl = t_load()

    template = {"yearID": "1871"}

    print("update by template\n" + json.dumps(template, indent=2))

    result = csv_tbl.update_by_template(template, {"teamID": "ATL", "AB": "11"})

    if result is not None:
        print("%d rows updated" % result)
    else:
        print("None.")


# update_by_template()


def insert():

    csv_tbl = t_load()

    result = csv_tbl.insert({"playerID": "test1", "yearID": "test1", "stint": "1", "teamID": "test"})

    if result is None:
        print("insert successfully")


insert()

