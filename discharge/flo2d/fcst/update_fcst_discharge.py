import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.discharge import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.logger import logger


if __name__=="__main__":

    try:

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        TS = Timeseries(pool=curw_sim_pool)

        method = MethodEnum.getAbbreviation(MethodEnum.SF)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/discharge_stations/extract_stations.csv')
        extract_stations_dict = { }  # keys: station_name , value: [latitude, longitude, target_method]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][3]]

        for station_name in extract_stations_dict.keys():
            meta_data = {
                'latitude': float('%.6f' % float(extract_stations_dict.get(station_name)[0])),
                'longitude': float('%.6f' % float(extract_stations_dict.get(station_name)[1])),
                'model': extract_stations_dict.get(station_name)[2], 'method': method,
                'grid_id': 'discharge_{}'.format(station_name)
            }

            tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

            timeseries = read_csv('discharge/flo2d/fcst/{}.csv'.format(station_name))

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                TS.insert_run(meta_data=meta_data)

            existing_ts_end = TS.get_timeseris_end(id_=tms_id)

            discharge_ts = []

            if existing_ts_end is None:
                discharge_ts = timeseries
            else:
                for i in range(len(timeseries)):
                    if (timeseries[i][0]).strptime(COMMON_DATE_TIME_FORMAT) > existing_ts_end:
                        discharge_ts = timeseries[i:]
                        break

            if discharge_ts is not None and len(discharge_ts) > 0:
                TS.insert_data(timeseries=discharge_ts, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred")
    finally:
        destroy_Pool(pool=curw_sim_pool)
