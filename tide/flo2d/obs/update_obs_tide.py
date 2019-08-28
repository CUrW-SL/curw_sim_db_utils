import csv
import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.tide import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_sim.common import append_ts, average_timeseries
from db_adapter.logger import logger


TEMP_HOST = "10.138.0.6"
TEMP_USER = "root"
TEMP_PASSWORD = "cfcwm07"
TEMP_DB ="curw"
TEMP_PORT = 3306

OBS_STATIONS_LIST = ['curw_wl_wellawatta', 'curw_wl_mattakkuliya']


def prepare_obs_tide_ts(connection, start_date, end_date):

    tide_ts = []

    # create hourly timeseries
    timestamp = start_date
    while timestamp <= end_date:
        tide_ts.append([timestamp.strftime(COMMON_DATE_TIME_FORMAT)])
        timestamp = timestamp + timedelta(hours=1)

    for station in OBS_STATIONS_LIST:
        ts = []
        with connection.cursor() as cursor1:
            cursor1.callproc('getWL', (station, start_date, end_date))
            results = cursor1.fetchall()
            for result in results:
                ts.append([result.get('time').strftime(COMMON_DATE_TIME_FORMAT), result.get('value')])

        tide_ts = append_ts(original_ts=tide_ts, new_ts=ts)

    avg_tide_ts = average_timeseries(tide_ts)

    return avg_tide_ts


if __name__ == "__main__":

    try:

        curw_pool = get_Pool(host=TEMP_HOST, user=TEMP_USER, password=TEMP_PASSWORD,
                port=TEMP_PORT, db=TEMP_DB)

        connection = curw_pool.connection()

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        method = MethodEnum.getAbbreviation(MethodEnum.TSF)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/tide_stations/extract_stations.csv')
        extract_stations_dict = {}  # keys: station_name , value: [latitude, longitude, target_method]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][3]]

        station_name = 'colombo'

        meta_data = {
            'latitude': float('%.6f' % float(extract_stations_dict.get(station_name)[0])),
            'longitude': float('%.6f' % float(extract_stations_dict.get(station_name)[1])),
            'model': extract_stations_dict.get(station_name)[2], 'method': method,
            'grid_id': 'tide_{}'.format(station_name)
        }

        TS = Timeseries(pool=curw_sim_pool)

        tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

        start_date = datetime.strptime((datetime.now()- timedelta(days=10)).strftime('%Y-%m-%d %H:00:00'), COMMON_DATE_TIME_FORMAT)
        end_date = datetime.now() + timedelta(hours=5, minutes=30)

        final_tide_ts = []

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=meta_data)
            meta_data['id'] = tms_id
            TS.insert_run(meta_data=meta_data)
            final_tide_ts = prepare_obs_tide_ts(connection=connection, start_date=start_date, end_date=end_date)
        else:
            start_date = datetime.strptime((datetime.now() - timedelta(hours=20)).strftime('%Y-%m-%d %H:00:00'),
                                           COMMON_DATE_TIME_FORMAT)
            final_tide_ts = prepare_obs_tide_ts(connection=connection, start_date=start_date, end_date=end_date)

        for i in range(len(final_tide_ts)):
            if len(final_tide_ts[i]) < 2:
                final_tide_ts.remove(final_tide_ts[i])

        if final_tide_ts is not None and len(final_tide_ts) > 0:
            TS.insert_data(timeseries=final_tide_ts, tms_id=tms_id, upsert=True)
            TS.update_latest_obs(id_=tms_id, obs_end=final_tide_ts[-1][0])

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred.")
    finally:
        connection.close()
        destroy_Pool(pool=curw_pool)
        destroy_Pool(pool=curw_sim_pool)

