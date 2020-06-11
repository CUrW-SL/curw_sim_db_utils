import traceback
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.tide import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.logger import logger

INPUT_DIR = "/mnt/disks/wrf_nfs/curw_sim_db_utils/tide/flo2d/fcst"


def list_of_lists_to_df_first_row_as_columns(data):
    return pd.DataFrame.from_records(data[1:], columns=data[0])


def extract_ts_from(start_date, timeseries):
    """
    timeseries from start date (exclusive)
    :param start_date:
    :param timeseries:
    :return:
    """

    output_ts = []

    for i in range(len(timeseries)):
        if timeseries[i][0] > start_date:
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


def process_tide_fcsts_from_Mobile_Geographics(existing_ts_end, obs_end):
    data = read_csv('{}/{}.csv'.format(INPUT_DIR, 'colombo_MGF'))
    raw_timeseries = []

    expected_fcst_end = datetime.strptime((datetime.now() + timedelta(days=60)).strftime("%Y-%m-01 00:00:00"),
                                          COMMON_DATE_TIME_FORMAT)

    if existing_ts_end is None or existing_ts_end < expected_fcst_end:

        for i in range(len(data)):
            time = datetime.strptime("{} {} {}".format(data[i][0], data[i][1], data[i][2]), "%m/%d/%Y %I:%M %p")
            formatted_time = time.strftime(COMMON_DATE_TIME_FORMAT)
            raw_timeseries.append([formatted_time, data[i][3]])

        if existing_ts_end is not None:
            fcst_start = (existing_ts_end).strftime(COMMON_DATE_TIME_FORMAT)
        elif obs_end is not None:
            fcst_start = (obs_end).strftime(COMMON_DATE_TIME_FORMAT)
        else:
            fcst_start = ((datetime.now() - timedelta(days=30))).strftime("%Y-%m-%d 00:00:00")

        raw_timeseries = extract_ts_from(fcst_start, raw_timeseries)
        raw_timeseries.insert(0, ['time', 'value'])

        timeseries_df = list_of_lists_to_df_first_row_as_columns(raw_timeseries)
        timeseries_df['time'] = pd.to_datetime(timeseries_df['time'], format=COMMON_DATE_TIME_FORMAT)
        timeseries_df['time'] = timeseries_df['time'].dt.round('h')
        # timeseries_df.set_index('time', inplace=True)
        #
        # timeseries_df['value'] = pd.to_numeric(timeseries_df['value'])
        # hourly_ts_df = timeseries_df.resample('H').asfreq()
        # hourly_ts_df = hourly_ts_df.interpolate(method='linear', limit_direction='both', limit=100) ####temp###
        # hourly_ts_df = hourly_ts_df.fillna(-99999.000)
        # hourly_ts_df.index = hourly_ts_df.index.map(str)

        fcst_end = (timeseries_df['time'].max()).strftime(COMMON_DATE_TIME_FORMAT)

        df = (pd.date_range(start=fcst_start, end=fcst_end, freq='60min')).to_frame(name='time')

        hourly_ts_df = pd.merge(df, timeseries_df, on='time', how='left')

        hourly_ts_df.interpolate(method='linear', limit_direction='both', limit=100)
        hourly_ts_df.fillna(inplace=True, value=0)

        hourly_ts_df['time'] = hourly_ts_df['time'].dt.strftime(COMMON_DATE_TIME_FORMAT)

        pd.set_option('display.max_rows', hourly_ts_df.shape[0] + 2)
        pd.set_option('display.max_columns', hourly_ts_df.shape[1] + 2)

        processed_timeseries = hourly_ts_df.reset_index().values.tolist()
        return processed_timeseries
    else:
        return None


if __name__=="__main__":

    try:
        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/tide_stations/extract_stations.csv')
        extract_stations_dict = {}  # keys: station_name , value: [latitude, longitude, target_method]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][3]]

        for station_name in extract_stations_dict.keys():
            fcst_station_name = None

            methods = []
            if station_name in ('colombo', 'mattakkuliya'):
                fcst_station_name = 'colombo' #temporary#
                methods.append(MethodEnum.getAbbreviation(MethodEnum.TSF))
                methods.append(MethodEnum.getAbbreviation(MethodEnum.MGF))
            elif station_name in ('wellawatta'):
                fcst_station_name = 'wellawatta'
                methods.append(MethodEnum.getAbbreviation(MethodEnum.TSF))
                methods.append(MethodEnum.getAbbreviation(MethodEnum.MGF))
            else:
                continue #skip current iteration

            for method in methods:

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

                obs_end = TS.get_obs_end(id_=tms_id)

                ts_end = TS.get_timeseries_end(id_=tms_id)

                processed_tide_ts = []

                if method in ('TSF'):
                    timeseries = read_csv('{}/{}.csv'.format(INPUT_DIR, fcst_station_name))

                    tide_ts = []

                    if obs_end is None:
                        tide_ts = timeseries
                    else:
                        tide_ts = extract_ts_from(start_date=obs_end.strftime(COMMON_DATE_TIME_FORMAT), timeseries=timeseries)

                    for i in range(len(tide_ts)):
                        processed_tide_ts.append([round_to_nearest_hour(tide_ts[i][0]), '%.3f' % float(tide_ts[i][1])])

                elif method in ('MGF'):
                    processed_tide_ts = process_tide_fcsts_from_Mobile_Geographics(existing_ts_end=ts_end, obs_end=obs_end)

                if processed_tide_ts is not None and len(processed_tide_ts) > 0:
                    TS.insert_data(timeseries=processed_tide_ts, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred")
    finally:
        destroy_Pool(pool=curw_sim_pool)
