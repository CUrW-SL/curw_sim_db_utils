#!/home/uwcc-admin/curw_sim_db_utils/venv/bin/python3
import traceback
from datetime import datetime, timedelta

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_PASSWORD, CURW_SIM_DATABASE, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.constants import HecHMS
from db_adapter.curw_sim.timeseries import MethodEnum, Timeseries
# from flush_data.flush_curw_sim_data_common import Timeseries, get_curw_sim_hash_ids

import sys
sys.path.insert(0, '/home/uwcc-admin/flush_data')
import flush_curw_sim_data_common as flush_common

if __name__=="__main__":

    try:

        pool = get_Pool(host=CURW_SIM_HOST, port=CURW_SIM_PORT, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                        db=CURW_SIM_DATABASE)

        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        run_table = "run"
        data_table = "data"
        end = (datetime.now() - timedelta(days=51)).strftime("%Y-%m-%d %H:%M:00")

        hash_ids = flush_common.get_curw_sim_hash_ids(pool=pool, run_table=run_table, model=HecHMS, method=method, obs_end_start=None,
                                         obs_end_end=None, grid_id=None)

        TS = flush_common.Timeseries(pool=pool, run_table=run_table, data_table=data_table)

        TS_rain = Timeseries(pool)

        ###########################################################################
        # Delete run entries without any observed data within last 50 days period #
        ###########################################################################
        count = 0
        for id in hash_ids:
            obs_end = TS_rain.get_obs_end(id)
            if (obs_end is None) or (obs_end.strftime("%Y-%m-%d %H:%M:%S") < end):
                count += 1
                TS.delete_all_by_hash_id(id)
                print(count, id)

        #####################################################################################################
        # delete a specific timeseries defined by a given hash id from data table for specified time period #
        #####################################################################################################
        # count = 0
        # for id in hash_ids:
        #     TS.delete_timeseries(id_=id, end=end)
        #     print(count, id)
        #     count += 1
        # print("{} of hash ids are deleted".format(len(hash_ids)))


        ##########################################################################################################
        # bulk delete a specific timeseries defined by a given hash id from data table for specified time period #
        ##########################################################################################################
        TS.bulk_delete_timeseries(ids=hash_ids, end=end)

    except Exception as e:
        print('An exception occurred.')
        traceback.print_exc()
    finally:
        print("Process finished")
        destroy_Pool(pool=pool)