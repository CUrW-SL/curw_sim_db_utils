import csv
import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.waterlevel import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_sim.common import fill_ts_missing_entries
from db_adapter.logger import logger

if __name__ == "__main__":

    try:

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        method = MethodEnum.getAbbreviation(MethodEnum.SF)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/discharge_stations/extract_stations.csv')
        extract_stations_dict = {}  # keys: station_name , value: [latitude, longitude, target_method]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][3]]

        station_name = 'hanwella'

        meta_data = {
            'latitude': float('%.6f' % float(extract_stations_dict.get(station_name)[0])),
            'longitude': float('%.6f' % float(extract_stations_dict.get(station_name)[1])),
            'model': extract_stations_dict.get(station_name)[2], 'method': method,
            'grid_id': 'waterlevel_{}'.format(station_name)
        }

        TS = Timeseries(pool=curw_sim_pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

        start_date = (datetime.now() - timedelta(days=10)).strftime(COMMON_DATE_TIME_FORMAT)

        filled_ts = []

        if tms_id is None:
            exit(0)
        else:
            end_date = TS.get_timeseries_end(id_=tms_id)
            original_ts = TS.get_timeseries(id_=tms_id, start_date=start_date, end_date=end_date)
            filled_ts = fill_ts_missing_entries(start=start_date, end=end_date, timeseries=original_ts,
                                                interpolation_method='linear', timestep=60)

        for i in range(len(filled_ts)):
            if filled_ts[i][1] < 0.2:
                filled_ts[i][1] = 0.2

        if filled_ts is not None and len(filled_ts) > 0:
            TS.insert_data(timeseries=filled_ts, tms_id=tms_id, upsert=True)
            TS.update_latest_obs(id_=tms_id, obs_end=filled_ts[-1][1])

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred.")
    finally:
        destroy_Pool(pool=curw_sim_pool)


