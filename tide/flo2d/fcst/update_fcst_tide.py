import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.tide import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.logger import logger


def extract_ts_from(start_date, timeseries):
    """
    timeseries from start date (inclusive)
    :param start_date:
    :param timeseries:
    :return:
    """

    output_ts = []

    for i in range(len(timeseries)):
        if timeseries[i][0] >= start_date:
            output_ts = timeseries[i:]
            break

    return output_ts


def round_to_nearest_hour(datetime_string, format=None):

    if format is None:
        time = datetime.strptime(datetime_string, COMMON_DATE_TIME_FORMAT)
    else:
        time = datetime.strptime(datetime_string, format)

    if time.minute > 30:
        return (time + timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")

    return time.strftime("%Y-%m-%d %H:00:00")


if __name__=="__main__":

    try:
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

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=meta_data)
            meta_data['id'] = tms_id
            TS.insert_run(meta_data=meta_data)

        timeseries = read_csv('tide/flo2d/fcst/{}.csv'.format(station_name))

        tide_ts = []

        existing_ts_end = TS.get_obs_end(id_=tms_id)

        if existing_ts_end is None:
            tide_ts = timeseries
        else:
            tide_ts = extract_ts_from(start_date=existing_ts_end.strftime(COMMON_DATE_TIME_FORMAT), timeseries=timeseries)

        print("tide ts, ", tide_ts)
        processed_tide_ts = []

        for i in range(len(tide_ts)):
            print(i)
            processed_tide_ts.append([round_to_nearest_hour(tide_ts[i][0]), tide_ts[i][1]])

        print("processed ts, ", processed_tide_ts)

        if processed_tide_ts is not None and len(processed_tide_ts) > 0:
            TS.insert_data(timeseries=processed_tide_ts, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred")
    finally:
        destroy_Pool(pool=curw_sim_pool)
