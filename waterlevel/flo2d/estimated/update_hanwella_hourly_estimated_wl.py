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
from db_adapter.logger import logger


TEMP_HOST = "10.138.0.6"
TEMP_USER = "root"
TEMP_PASSWORD = "cfcwm07"
TEMP_DB ="curw"
TEMP_PORT = 3306


if __name__ == "__main__":

    try:

        curw_pool = get_Pool(host=TEMP_HOST, user=TEMP_USER, password=TEMP_PASSWORD,
                port=TEMP_PORT, db=TEMP_DB)

        connection = curw_pool.connection()

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

        ranwala_ts = []
        hanwella_ts = []
        start_date = (datetime.now() - timedelta(days=10)).strftime(COMMON_DATE_TIME_FORMAT)
        end_date = (datetime.now().strftime(COMMON_DATE_TIME_FORMAT))

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=meta_data)
            meta_data['id'] = tms_id
            TS.insert_run(meta_data=meta_data)
            with connection.cursor() as cursor1:
                cursor1.callproc('getWL', ('curw_wl_ranwala', start_date, end_date))
                results = cursor1.fetchall()

                for result in results:
                    ranwala_ts.append([result.get('time'), result.get('value')])
        else:
            with connection.cursor() as cursor1:
                cursor1.callproc('getLatest24hrWaterLevel', ('curw_wl_ranwala',))
                results = cursor1.fetchall()
                for result in results:
                    ranwala_ts.append([result.get('time'), result.get('value')])

        for i in range(len(ranwala_ts) - 1):
            # x = Ranwala
            # DX Ranwala (x[x] - x[t-1]}
            # Hanwella = X 1.642174610188251` -   DX 3.8585516925010444` -
            #    8.810870547723741`;
            x = float(ranwala_ts[i + 1][1])
            dx = float(ranwala_ts[i + 1][1] - ranwala_ts[i][1])
            hanwella_wl = x * 1.642174610188251 - dx * 3.8585516925010444 - 8.810870547723741
            hanwella_ts.append([ranwala_ts[i + 1][0], '%.3f' % hanwella_wl])

        for i in range(len(hanwella_ts)):
            if hanwella_ts[i][1] < 0.2:
                hanwella_ts[i][1] = 0.2

        if hanwella_ts is not None and len(hanwella_ts) > 0:
            TS.insert_data(timeseries=hanwella_ts, tms_id=tms_id, upsert=True)
            TS.update_latest_obs(id_=tms_id, obs_end=hanwella_ts[-1][1])

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred.")
    finally:
        connection.close()
        destroy_Pool(pool=curw_pool)
        destroy_Pool(pool=curw_sim_pool)


