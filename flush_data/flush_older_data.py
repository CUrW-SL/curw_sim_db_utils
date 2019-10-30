import traceback
from datetime import datetime, timedelta


class Timeseries:
    def __init__(self, pool, data_table, run_table):
        self.pool = pool
        self.data_table = data_table
        self.run_table = run_table

    def delete_timeseries(self, id_, start=None, end=None):
        """
        Delete specific timeseries identified by hash id and a fgt
        :param id_: hash id
        :param fgt: fgt
        :return:
        """

        connection = self.pool.connection()
        data_table = self.data_table

        pre_sql_statement = "DELETE FROM `curw_sim`.`" + data_table + "` WHERE `id`= %s"

        condition_list = []
        variable_list = []

        variable_list.append(id_)

        if start is not None:
            condition_list.append("`time`>=%s")
            variable_list.append(start)
        if end is not None:
            condition_list.append("`time`<=%s")
            variable_list.append(end)

        conditions = " AND ".join(condition_list)

        sql_statement = pre_sql_statement + conditions + ";"

        try:
            with connection.cursor() as cursor:
                row_count = cursor.execute(sql_statement, tuple(variable_list))

            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of timeseries with hash id {} failed".format(id_)
            print(error_message)
            traceback.print_exc()
        finally:
            if connection is not None:
                connection.close()

