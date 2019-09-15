

import copy
import csv
import logging
import json
import os
import pandas as pd

from src import CSVHelper
from src.BaseDataTable import BaseDataTable


class CSVDataTable(BaseDataTable):
    """
        The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
        base class and implement the abstract methods.
        """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """
        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

        self._field_names = self._rows[0].keys()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._fn = full_name

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

        with open(self._fn, 'w') as out_file:
            wr = csv.DictWriter(out_file, fieldnames=self._field_names)
            wr.writeheader()
            for row in self._rows:
                wr.writerow(row)

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        row = CSVHelper.matches_table_key(self._rows, self._data["key_columns"], key_fields)

        if row is None:
            return None

        result = CSVHelper.get_columns(self._rows[row], field_list)

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

        result_dict = CSVHelper.matches_table_template(self._rows, template)

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

        result = CSVHelper.matches_table_key(self._rows, self._data["key_columns"], key_fields)

        if result is None:
            return None

        del self._rows[result]

        save()

        return 1

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        result_dict = CSVHelper.matches_table_template(self._rows, template)

        for result in result_dict.values():
            self._rows.remove(result)

        CSVHelper.commit(self._rows, self._fn)

        return len(result_dict)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        result = CSVHelper.matches_table_key(self._rows, self._data["key_columns"], key_fields)

        if result is None:
            return None

        for k, v in new_values.items():
            self._rows[result][k] = v

        CSVHelper.commit(self._rows, self._fn)

        return 1

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        result_dict = CSVHelper.matches_table_template(self._rows, template)

        remove_len = 0

        for i in result_dict.keys():
            flag = True
            for k, v in new_values.items():
                if self._rows[i][k] != v:
                    flag = False
                    self._rows[i][k] = v

            if flag is True:
                remove_len += 1

        CSVHelper.commit(self._rows, self._fn)

        return len(result_dict)-remove_len

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        result_dict = CSVHelper.matches_table_template(self._rows, new_record)

        if len(result_dict) != 0:
            print("record already existed")
            return 1

        self._rows.append(new_record)

        CSVHelper.commit(self._rows, self._fn)

        return None

    def get_rows(self):
        return self._rows

