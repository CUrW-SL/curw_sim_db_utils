import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.discharge import Timeseries as DTimeseries
from db_adapter.curw_sim.timeseries.waterlevel import Timeseries as WLTimeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.logger import logger


def calculate_hanwella_discharge(hanwella_wl_ts):
    discharge_ts = []
    for i in range(len(hanwella_wl_ts)):
        wl = float(hanwella_wl_ts[i][1])
        discharge = 26.1131 * (wl ** 1.73499)
        discharge_ts.append([hanwella_wl_ts[i][0], '%.3f' % discharge])

    return discharge_ts


def calculate_glencourse_discharge(glencourse_wl_ts):
    """Q = 41.904 (H – 7.65)^1.518 (H&lt;=16.0 m AMSL)
Q = 21.323 (H – 7.71)^1.838 (H&gt;16.0 m AMSL)"""
    discharge_ts = []
    for i in range(len(glencourse_wl_ts)):
        wl = float(glencourse_wl_ts[i][1])
        if wl <= 16 :
            discharge = 41.904 * ((wl - 7.65) ** 1.518)
        else:
            discharge = 21.323 * ((wl - 7.71) ** 1.838)
        discharge_ts.append([glencourse_wl_ts[i][0], '%.3f' % discharge])

    return discharge_ts


if __name__=="__main__":

    try:

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        discharge_TS = DTimeseries(pool=curw_sim_pool)
        waterlevel_TS = WLTimeseries(pool=curw_sim_pool)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/discharge_stations/flo2d_stations.csv')
        # extract_stations_dict = { }  # keys: station_name , value: [latitude, longitude, target_model]

        # for obs_index in range(len(extract_stations)):
        #     extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
        #                                                              extract_stations[obs_index][2],
        #                                                              extract_stations[obs_index][3],
        #                                                              extract_stations[obs_index][4]]

        for i in range(len(extract_stations)):

            station_name = extract_stations[i][0]
            latitude = extract_stations[i][1]
            longitude = extract_stations[i][2]
            target_model = extract_stations[i][3]
            method = extract_stations[i][4]
            wl_method = extract_stations[i][9]

            meta_data = {
                'latitude': float('%.6f' % float(latitude)),
                'longitude': float('%.6f' % float(longitude)),
                'model': target_model, 'method': method,
                'grid_id': 'discharge_{}'.format(station_name)
            }

            wl_meta_data = {
                'latitude': float('%.6f' % float(latitude)),
                'longitude': float('%.6f' % float(longitude)),
                'model': 'flo2d', 'method': wl_method,
                'grid_id': 'waterlevel_{}'.format(station_name)
            }

            tms_id = discharge_TS.get_timeseries_id_if_exists(meta_data=meta_data)
            wl_tms_id = waterlevel_TS.get_timeseries_id_if_exists(meta_data=wl_meta_data)

            if wl_tms_id is None:
                print("Warning!!! {} waterlevel timeseries doesn't exist.".format(station_name))
                continue

            end_time = (datetime.now() + timedelta(hours=5, minutes=30)).strftime(COMMON_DATE_TIME_FORMAT)

            timeseries = []

            if tms_id is None:
                tms_id = discharge_TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                discharge_TS.insert_run(meta_data=meta_data)
                start = (datetime.now() - timedelta(days=10)).strftime(COMMON_DATE_TIME_FORMAT)
            else:
                obs_end = discharge_TS.get_obs_end(id_=tms_id)
                if obs_end is None:
                    start = (datetime.now() - timedelta(days=10)).strftime(COMMON_DATE_TIME_FORMAT)
                else:
                    start = (obs_end - timedelta(days=1)).strftime(COMMON_DATE_TIME_FORMAT)

            wl_timeseries = waterlevel_TS.get_timeseries(id_=wl_tms_id, start_date=start, end_date=end_time)

            estimated_discharge_ts = []

            if station_name == 'hanwella':
                estimated_discharge_ts = calculate_hanwella_discharge(wl_timeseries)
            elif station_name == 'glencourse':
                estimated_discharge_ts = calculate_glencourse_discharge(wl_timeseries)

            if estimated_discharge_ts is not None and len(estimated_discharge_ts) > 0:
                discharge_TS.insert_data(timeseries=estimated_discharge_ts, tms_id=tms_id, upsert=True)
                discharge_TS.update_latest_obs(id_=tms_id, obs_end=estimated_discharge_ts[-1][1])

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred")
    finally:
        destroy_Pool(pool=curw_sim_pool)
