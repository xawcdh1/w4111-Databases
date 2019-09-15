import csv
import ast


def load_csv_file(fn):

    result = []
    with open(fn, 'r') as in_file:
        csv_file = csv.DictReader(in_file)
        for r in csv_file:
            result.append(r)

        return result


def commit(table, fn):

    if len(table) != 0:
        filed_names = table[0].keys()

    with open(fn, 'w') as out_file:
        wr = csv.DictWriter(out_file, fieldnames=filed_names)
        wr.writeheader()
        for row in table:
            wr.writerow(row)


def get_columns(row, col_list):

    if col_list is None:
        return row

    result = {}
    for c in col_list:
        result[c] = row[c]

    return result


def matches_table_template(table, template):

    find = True
    result_dict = {}

    for i, row in enumerate(table):
        find = True
        for k, v in template.items():
            if v != row.get(k, None):
                find = False
                break
        if find is True:
            result_dict[i] = row

    return result_dict


def matches_table_key(table, key_columns, key_fields):

    key_dicts = dict(zip(key_columns, key_fields))

    for i, row in enumerate(table):
        find = True
        for k, v in key_dicts.items():
            if v != row.get(k, None):
                find = False
                break
        if find is True:
            return i

    return None


def delete_table_key(table, key_columns, key_fields):

    key_dicts = dict(zip(key_columns, key_fields))

    find = True

    for row in table:
        for k, v in key_dicts:
            if v != row.get(k, None):
                find = False
                break
        if find is True:
            table.remove(row)

