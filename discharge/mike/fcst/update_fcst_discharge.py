import traceback
import pandas as pd
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_HOST, CURW_FCST_PASSWORD, CURW_FCST_PORT, CURW_FCST_USERNAME
from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.timeseries.discharge import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_fcst.timeseries import Timeseries as Fcst_Timeseries
from db_adapter.curw_fcst.source import get_source_id
from db_adapter.logger import logger


INPUT_DIR = "/mnt/disks/wrf_nfs/curw_sim_db_utils/discharge/mike/fcst"

latest_fgt = None


def round_to_nearest_hour(datetime_string, format=None):

    if format is None:
        time = datetime.strptime(datetime_string, COMMON_DATE_TIME_FORMAT)
    else:
        time = datetime.strptime(datetime_string, format)

    if time.minute > 30:
        return (time + timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")

    return time.strftime("%Y-%m-%d %H:00:00")


def list_of_lists_to_df_first_row_as_columns(data):
    """

    :param data: data in list of lists format
    :return: equivalent pandas dataframe
    """

    return pd.DataFrame.from_records(data[1:], columns=data[0])


def process_fcst_ts_from_flo2d_outputs(curw_fcst_pool, fcst_start):

    global latest_fgt

    FCST_TS = Fcst_Timeseries(curw_fcst_pool)

    try:
        # [station_name,latitude,longitude,target,model,version,sim_tag,station]
        source_model = extract_stations[i][4]
        version = extract_stations[i][5]
        sim_tag = extract_stations[i][6]
        station_id = extract_stations[i][7]

        variable_id = 3 # Discharge
        unit_id = 3 # m3/s | Instantaneous

        source_id = get_source_id(pool=curw_fcst_pool, model=source_model, version=version)

        fcst_series = FCST_TS.get_latest_timeseries(sim_tag, station_id, source_id, variable_id, unit_id, start=None)
        if (fcst_series is None) or (len(fcst_series)<1):
            return None

        latest_fgt = (FCST_TS.get_end_date(sim_tag, station_id, source_id, variable_id, unit_id)).strftime(COMMON_DATE_TIME_FORMAT)

        fcst_series.insert(0, ['time', 'value'])
        fcst_df = list_of_lists_to_df_first_row_as_columns(fcst_series)

        fcst_end = (fcst_df['time'].max()).strftime(COMMON_DATE_TIME_FORMAT)
        if fcst_start is None:
            fcst_start = (fcst_df['time'].min()).strftime(COMMON_DATE_TIME_FORMAT)

        df = (pd.date_range(start=fcst_start, end=fcst_end, freq='15min')).to_frame(name='time')

        processed_df = pd.merge(df, fcst_df, on='time', how='left')

        # processed_df.interpolate(method='linear', limit_direction='both')
        processed_df = processed_df.dropna()
        processed_df['time'] = processed_df['time'].dt.strftime(COMMON_DATE_TIME_FORMAT)

        return processed_df.values.tolist()

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred")


if __name__=="__main__":

    try:

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        curw_fcst_pool = get_Pool(host=CURW_FCST_HOST, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                                  port=CURW_FCST_PORT, db=CURW_FCST_DATABASE)

        TS = Timeseries(pool=curw_sim_pool)

        # [station_name,latitude,longitude,target,model,version,sim_tag,station]
        extract_stations = read_csv('grids/discharge_stations/mike_stations.csv')

        for i in range(len(extract_stations)):
            station_name = extract_stations[i][0]
            latitude = extract_stations[i][1]
            longitude = extract_stations[i][2]
            target_model = extract_stations[i][3]

            if station_name in ('ambatale'):
                method = MethodEnum.getAbbreviation(MethodEnum.MME)
            else:
                continue  ## skip the current station and move to next iteration

            meta_data = {
                'latitude': float('%.6f' % float(latitude)),
                'longitude': float('%.6f' % float(longitude)),
                'model': target_model, 'method': method,
                'grid_id': 'discharge_{}'.format(station_name)
            }

            tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                TS.insert_run(meta_data=meta_data)

            # obs_end = TS.get_obs_end(id_=tms_id)

            processed_discharge_ts = []


            if station_name in ('ambatale'):    # process fcst ts from model outputs
                processed_discharge_ts = process_fcst_ts_from_flo2d_outputs(curw_fcst_pool=curw_fcst_pool,
                                                                             fcst_start=None)

            else:
                continue  ## skip the current station and move to next iteration

            if processed_discharge_ts is not None and len(processed_discharge_ts) > 0:
                TS.insert_data(timeseries=processed_discharge_ts, tms_id=tms_id, upsert=True)
                TS.update_latest_obs(id_=tms_id, obs_end=latest_fgt)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred")
    finally:
        destroy_Pool(pool=curw_sim_pool)
        destroy_Pool(pool=curw_fcst_pool)

