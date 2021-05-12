from typing import List, Any

from psycopg2 import connect
from psycopg2.extensions import register_adapter
from psycopg2.extras import Json, execute_values, execute_batch

from app.utils import getLogger

logger = getLogger(__name__)


class Postgres:
    def __init__(self, user=None, password=None, host=None, dbname=None):
        self.conn = connect(user=user, password=password, host=host, dbname=dbname)

    def update(
        self,
        table: str,
        data: dict,
        col_to_filter_on: str,
        schema: str = "public",
    ):
        # Remove the value you are filtering as that will be used in where clause
        # filter_value = data.pop(col_to_filter_on, None)

        cols_to_update = list(data.keys())
        # cols_to_update.remove(col_to_filter_on)
        # records = list(data.values())
        # records.append(filter_value)
        # tuple_records = [(k, v) for k, v in data.items()]
        tuple_records = [(i,) for i in data.values()]

        set_statement = ", ".join([f'"{val}" = %({val})s' for val in cols_to_update])
        with self.conn:
            with self.conn.cursor() as curs:
                sql_update_qry = f"""Update {schema}."{table}" SET {set_statement}
                                      WHERE "{table}"."{col_to_filter_on}" = %({col_to_filter_on})s"""

                logger.info("sql_update_qry", tuple_records)
                curs.executemany(sql_update_qry, (data,))
                self.conn.commit()

    def update_in_bulk(
        self,
        table: str,
        records: List[tuple],
        cols_to_update: list,
        col_to_filter_on: str,
        schema: str = "public",
        page_size=10000,
    ):
        col_to_filter_alias = f"cf_{col_to_filter_on}"
        set_statement = ", ".join([f"{val} = data.{val}" for val in cols_to_update])
        columns = (col_to_filter_alias,) + tuple(cols_to_update)
        with self.conn:
            with self.conn.cursor() as curs:
                qry = f"""UPDATE "{table}" as t SET {set_statement}
                          FROM (VALUES %s) as data {str(columns).replace("'", "")}
                          WHERE data."{col_to_filter_alias}" = t.{col_to_filter_on}"""

                execute_batch(curs, qry, records, page_size=page_size)

    def insert_bulk(
        self, table: str, columns: list, data: List[tuple], schema="public", cast_as_json=False, page_size=10000
    ):
        if not isinstance(columns, list):
            raise TypeError("columns must be a list")

        if not isinstance(data, list):
            raise TypeError("data must be a list")

        if cast_as_json:
            register_adapter(dict, Json)
            register_adapter(list, Json)
            template = str(tuple(["%s::Json" if isinstance(x, list) else "%s" for x in data[0]])).replace("'", "")
        else:
            template = None

        with self.conn:
            with self.conn.cursor() as curs:
                q = f"""INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES %s """
                execute_values(curs, q, data, template=template, page_size=page_size)

    def query(self, query: str, tablename: str):
        with self.conn:
            with self.conn.cursor() as curs:
                curs.execute(
                    query,
                    (tablename,),
                )
                rows = curs.fetchall()
                return rows
