import pymysql
import logging

logger = logging.getLogger()


def get_default_connection():

    result = pymysql.connect(host='localhost',
                             user='dbuser',
                             password='dbuserdbuser',
                             db='lahman2019raw',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    return result


def run_q(sql, args=None, fetch=True, cur=None, conn=None, commit=True):

    """
    Helper function to run an SQL statement.

    :param sql: SQL template with placeholders for parameters.
    :param args: Values to pass with statement.
    :param fetch: Execute a fetch and return data.
    :param conn: The database connection to use. The function will use the default if None.
    :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
    :param commit: This is wizard stuff. Do not worry about it.

    :return: A tuple of the form (execute response, fetched data)
    """

    cursor_created = False
    connection_created = False

    try:

        if conn is None:
            connection_created = True
            conn = _get_default_connection()

        if cur is None:
            cursor_created = True
            cur = conn.cursor()

        if args is not None:
            log_message = cur.mogrify(sql, args)
        else:
            log_message = sql

        logger.debug("Executing SQL = " + log_message)

        res = cur.execute(sql, args)

        if fetch:
            data = cur.fetchall()
        else:
            data = None

        # Do not ask.
        if commit is True:
            conn.commit()

    except Exception as e:
        raise e

    return res, data


def template_to_clause(template):

    args = []
    terms = []

    for k, v in template.items():
        terms.append(" " + k + "=%s")
        args.append(v)

    w_clause = " AND".join(terms)
    w_clause = " WHERE" + w_clause

    return w_clause, args


def get_targeted_fields(field_list):

    if field_list is None:
        fields = " * "
    else:
        fields = " " + ",".join(field_list) + " "

    return fields


def key_columns_to_clause(key_columns):

    terms = []

    for field in key_columns:
        terms.append(" " + field + "=%s")

    clause = " AND".join(terms)
    clause = " WHERE" + clause

    return clause


def new_values_to_set(new_values):
    terms = []
    args = []

    for k, v in new_values.items():
        terms.append(" " + k + "=%s")
        args.append(v)

    s_clause = ",".join(terms)
    s_clause = " set" + s_clause

    return s_clause, args


def create_select_template(table_name, template, field_list):

    fields = get_targeted_fields(field_list)

    clause, args = template_to_clause(template)

    sql = "select" + fields + "from " + table_name + clause

    return sql, args


def create_delete_template(table_name, template):

    d_clause, d_args = template_to_clause(template)

    sql = "Delete from " + table_name + d_clause

    return sql, d_args


def create_update_key_fields(table_name, key_columns, key_fields, new_values):

    w_clause = key_columns_to_clause(key_columns)

    s_clause, s_args = new_values_to_set(new_values)

    sql = "Update " + table_name + s_clause + w_clause

    s_args.extend(key_fields)

    return sql, s_args


def create_update_template(table_name, template, new_values):

    w_clause, w_args = template_to_clause(template)

    s_clause, s_args = new_values_to_set(new_values)

    sql = "Update " + table_name + s_clause + w_clause

    s_args.extend(w_args)

    return sql, s_args


def create_insert(table_name, new_record):

    cols = []
    args = []

    for k, v in new_record.items():
        cols.append(k)
        args.append(v)

    c_clause = "(" + ",".join(cols) + ")"

    terms = ["%s"]*len(cols)
    terms = ",".join(terms)

    v_clause = " values " + "(" + terms + ")"

    sql = "Insert into " + table_name + " " + c_clause + v_clause

    return sql, args





