

import copy
import csv

from src import CSVHelper
from src.BaseDataTable import BaseDataTable


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._fn = connect_info['directory'] + "/" + connect_info['file_name']
        self._table = CSVHelper.load_csv_file(self._fn)

        self._key_columns = key_columns

        self._filed_names = self._table[0].keys()

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        row = CSVHelper.matches_table_key(self._table, self._key_columns, key_fields)

        if row is None:
            return None

        result = CSVHelper.get_columns(self._table[row], field_list)

        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        result_dict = CSVHelper.matches_table_template(self._table, template)

        if field_list is None:
            return result_dict.values()

        result_list = []
        for k, result in result_dict.items():
            result_list.append(CSVHelper.get_columns(result, field_list))

        return result_list

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :return: A count of the rows deleted.
        """

        result = CSVHelper.matches_table_key(self._table, self._key_columns, key_fields)

        if result is None:
            return None

        del self._table[result]

        CSVHelper.commit(self._table, self._fn)

        return 1

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        result_dict = CSVHelper.matches_table_template(self._table, template)

        for result in result_dict.values():
            self._table.remove(result)

        CSVHelper.commit(self._table, self._fn)

        return len(result_dict)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        result = CSVHelper.matches_table_key(self._table, self._key_columns, key_fields)

        if result is None:
            return None

        for k, v in new_values.items():
            self._table[result][k] = v

        CSVHelper.commit(self._table, self._fn)

        return 1

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        result_dict = CSVHelper.matches_table_template(self._table, template)

        remove_len = 0

        for i in result_dict.keys():
            flag = True
            for k, v in new_values.items():
                if self._table[i][k] != v:
                    flag = False
                    self._table[i][k] = v

            if flag is True:
                remove_len += 1

        CSVHelper.commit(self._table, self._fn)

        return len(result_dict)-remove_len

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        result_dict = CSVHelper.matches_table_template(self._table, new_record)

        if len(result_dict) != 0:
            print("record already existed")
            return 1

        self._table.append(new_record)

        CSVHelper.commit(self._table, self._fn)

        return None

    def get_rows(self):
        return self._rows

