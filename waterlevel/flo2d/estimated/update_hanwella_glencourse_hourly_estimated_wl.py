import csv
import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_OBS_DATABASE, CURW_OBS_PORT, CURW_OBS_PASSWORD, CURW_OBS_USERNAME, CURW_OBS_HOST
from db_adapter.curw_sim.timeseries.waterlevel import Timeseries
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_sim.common import fill_ts_missing_entries
from db_adapter.logger import logger

RANWALA_WL_ID = 100064


def calculate_hanwella_wl_from_ranwala(ranwala_ts):
    hanwella_ts = []
    "Han[i]=1.623194Ran[i]-5.16108(Ran[i]-Ran[i-1])-10.847356"

    for i in range(len(interpolated_ranwala_ts) - 1):
        # x = Ranwala
        # DX Ranwala (x[x] - x[t-1]}
        # Hanwella = X 1.642174610188251` -   DX 3.8585516925010444` -
        #    8.810870547723741`;
        x = float(ranwala_ts[i + 1][1])
        dx = float(ranwala_ts[i + 1][1] - ranwala_ts[i][1])
        hanwella_wl = x * 1.623194 - dx * 5.16108 - 10.847356
        hanwella_ts.append([ranwala_ts[i + 1][0], '%.3f' % hanwella_wl])

    for i in range(len(hanwella_ts)):
        if float(hanwella_ts[i][1]) < 0.2:
            hanwella_ts[i][1] = 0.2

    return hanwella_ts


def calculate_glencourse_wl_from_ranwala(ranwala_ts):
    "-5.908532+2.2784865x-0.0309476x2"
    glencourse_ts = []

    for i in range(len(ranwala_ts)):
        ranwala_wl = float(ranwala_ts[i][1])
        glencourse_wl = -5.908532 + (2.2784865 * ranwala_wl) - (0.0309476 * (ranwala_wl ** 2))
        glencourse_ts.append([ranwala_ts[i][0], '%.3f' % glencourse_wl])

    return glencourse_ts


if __name__ == "__main__":

    try:

        curw_obs_pool = get_Pool(host=CURW_OBS_HOST, user=CURW_OBS_USERNAME, password=CURW_OBS_PASSWORD,
                port=CURW_OBS_PORT, db=CURW_OBS_DATABASE)

        connection = curw_obs_pool.connection()

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        method = MethodEnum.getAbbreviation(MethodEnum.SF)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/waterlevel_stations/extract_stations.csv')
        extract_stations_dict = {}  # keys: station_name , value: [latitude, longitude, target_method]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][3]]

        for station_name in extract_stations_dict.keys():

            meta_data = {
                'latitude': float('%.6f' % float(extract_stations_dict.get(station_name)[0])),
                'longitude': float('%.6f' % float(extract_stations_dict.get(station_name)[1])),
                'model': extract_stations_dict.get(station_name)[2], 'method': method,
                'grid_id': 'waterlevel_{}'.format(station_name)
            }

            TS = Timeseries(pool=curw_sim_pool)

            tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

            ranwala_ts = []
            start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:00:00")
            end_date = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:00:00")

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                TS.insert_run(meta_data=meta_data)
            else:
                obs_end = TS.get_obs_end(id_=tms_id)
                start_date = (obs_end - timedelta(days=1)).strftime("%Y-%m-%d %H:00:00")

            with connection.cursor() as cursor1:
                cursor1.callproc('getWL', (RANWALA_WL_ID, start_date, end_date))
                results = cursor1.fetchall()
                for result in results:
                    ranwala_ts.append([result.get('time'), result.get('value')])

            interpolated_ranwala_ts = fill_ts_missing_entries(start=start_date, end=end_date, timeseries=ranwala_ts,
                                                    interpolation_method='linear', timestep=60)

            estimated_wl_ts = []

            if station_name == 'hanwella':
                estimated_wl_ts = calculate_hanwella_wl_from_ranwala(interpolated_ranwala_ts)
            elif station_name == 'glencourse':
                estimated_wl_ts = calculate_glencourse_wl_from_ranwala(interpolated_ranwala_ts)

            if estimated_wl_ts is not None and len(estimated_wl_ts) > 0:
                TS.insert_data(timeseries=estimated_wl_ts, tms_id=tms_id, upsert=True)
                TS.update_latest_obs(id_=tms_id, obs_end=estimated_wl_ts[-1][1])

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred.")
    finally:
        connection.close()
        destroy_Pool(pool=curw_obs_pool)
        destroy_Pool(pool=curw_sim_pool)


