import pymysql as pymysql

from src import SQLHelper
from src.BaseDataTable import BaseDataTable


class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, key_columns, connect_info):
        """

        :param table_name: Logical name of the table.
        :param connect_info: connection to database.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        self._table_name = table_name
        self._connect = connect_info

        # self._cursor = self.connect.cursor()

        self._key_columns = key_columns

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        fields = SQLHelper.get_targeted_fields(field_list)

        clause = SQLHelper.key_columns_to_clause(self._key_columns)

        sql = "select" + fields + "from " + self._table_name + clause

        result = SQLHelper.run_q(sql, key_fields, conn=self._connect)

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

        sql, args = SQLHelper.create_select_template(self._table_name, template, field_list)

        res, data = SQLHelper.run_q(sql, args, conn=self._connect)

        return res, data

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :return: A count of the rows deleted.
        """

        clause = SQLHelper.key_columns_to_clause(self._key_columns)

        sql = "Delete From " + self._table_name + clause

        res, data = SQLHelper.run_q(sql, key_fields, conn=self._connect)

        return res, data

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        sql, args = SQLHelper.create_delete_template(self._table_name, template)

        res, data = SQLHelper.run_q(sql, args, conn=self._connect)

        return res, data

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        sql, args = SQLHelper.create_update_key_fields(self._table_name, self._key_columns, key_fields, new_values)

        res, data = SQLHelper.run_q(sql, args, conn=self._connect)

        return res, data

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        sql, args = SQLHelper.create_update_template(self._table_name, template, new_values)

        res, data = SQLHelper.run_q(sql, args, conn=self._connect)

        return res, data

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql, args = SQLHelper.create_insert(self._table_name, new_record)

        res, data = SQLHelper.run_q(sql, args, conn=self._connect)

        return res, data

    def get_rows(self):
        return self._rows





