import traceback
from datetime import datetime, timedelta
from db_adapter.csv_utils import read_csv

from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_HOST
from db_adapter.constants import CURW_OBS_DATABASE, CURW_OBS_PASSWORD, CURW_OBS_USERNAME, CURW_OBS_PORT, CURW_OBS_HOST
from db_adapter.constants import COMMON_DATE_TIME_FORMAT

from db_adapter.curw_sim.grids import get_flo2d_cells_to_wrf_grid_mappings, get_flo2d_cells_to_obs_grid_mappings
from db_adapter.curw_sim.timeseries import Timeseries as Sim_Timeseries
from db_adapter.curw_fcst.timeseries import Timeseries as Fcst_Timeseries
from db_adapter.curw_fcst.source import get_source_id
from db_adapter.curw_sim.common import convert_15_min_ts_to_5_mins_ts, append_value_for_timestamp, average_timeseries, \
    process_5_min_ts, process_15_min_ts, fill_missing_values, \
    extract_obs_rain_5_min_ts, extract_obs_rain_15_min_ts

from db_adapter.logger import logger


# for bulk insertion for a given one grid interpolation method
def update_rainfall_fcsts(flo2d_model, method, grid_interpolation, model_list, timestep):

    """
    Update rainfall forecasts for flo2d models
    :param flo2d_model: flo2d model
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :param model_list: list of forecast model and their versions used to calculate the rainfall
    e.g.: [["WRF_E", "v4"],["WRF_SE", "v4"]]
    :param timestep: output timeseries timestep
    :return:
    """

    try:
        # Connect to the database
        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        curw_fcst_pool = get_Pool(host=CURW_FCST_HOST, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                port=CURW_FCST_PORT, db=CURW_FCST_DATABASE)

        Sim_TS = Sim_Timeseries(pool=curw_sim_pool)
        Fcst_TS = Fcst_Timeseries(pool=curw_fcst_pool)

        flo2d_grids = read_csv('grids/flo2d/{}m.csv'.format(flo2d_model))  # [Grid_ ID, X(longitude), Y(latitude)]

        flo2d_wrf_mapping = get_flo2d_cells_to_wrf_grid_mappings(pool=curw_sim_pool, grid_interpolation=grid_interpolation, flo2d_model=flo2d_model)

        for flo2d_index in range(len(flo2d_grids)):  # len(flo2d_grids)
            lat = flo2d_grids[flo2d_index][2]
            lon = flo2d_grids[flo2d_index][1]
            cell_id = flo2d_grids[flo2d_index][0]
            meta_data = {
                    'latitude': float('%.6f' % float(lat)), 'longitude': float('%.6f' % float(lon)),
                    'model': flo2d_model, 'method': method,
                    'grid_id': '{}_{}_{}'.format(flo2d_model, grid_interpolation, (str(cell_id)).zfill(10))
                    }

            tms_id = Sim_TS.get_timeseries_id(grid_id=meta_data.get('grid_id'), method=meta_data.get('method'))

            if tms_id is None:
                tms_id = Sim_TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                Sim_TS.insert_run(meta_data=meta_data)

            obs_end = Sim_TS.get_obs_end(id_=tms_id)

            fcst_timeseries = []

            for i in range(len(model_list)):
                source_id = get_source_id(pool=curw_fcst_pool, model=model_list[i][0], version=model_list[i][1])

                temp_timeseries = []

                if timestep == 5:
                    if obs_end is not None:
                        temp_timeseries = convert_15_min_ts_to_5_mins_ts(
                                newly_extracted_timeseries=Fcst_TS.get_latest_timeseries(
                                        sim_tag="evening_18hrs", station_id=flo2d_wrf_mapping.get(meta_data['grid_id']),
                                        start=obs_end,
                                        source_id=source_id, variable_id=1, unit_id=1))
                    else:
                        temp_timeseries = convert_15_min_ts_to_5_mins_ts(
                                newly_extracted_timeseries=Fcst_TS.get_latest_timeseries(
                                        sim_tag="evening_18hrs", station_id=flo2d_wrf_mapping.get(meta_data['grid_id']),
                                        source_id=source_id, variable_id=1, unit_id=1))
                elif timestep == 15:
                    if obs_end is not None:
                        temp_timeseries = Fcst_TS.get_latest_timeseries(
                                        sim_tag="evening_18hrs", station_id=flo2d_wrf_mapping.get(meta_data['grid_id']),
                                        start=obs_end,
                                        source_id=source_id, variable_id=1, unit_id=1)
                    else:
                        temp_timeseries = Fcst_TS.get_latest_timeseries(
                                        sim_tag="evening_18hrs", station_id=flo2d_wrf_mapping.get(meta_data['grid_id']),
                                        source_id=source_id, variable_id=1, unit_id=1)

                if i == 0:
                    fcst_timeseries = temp_timeseries
                else:
                    fcst_timeseries = append_value_for_timestamp(existing_ts=fcst_timeseries, new_ts=temp_timeseries)

            avg_timeseries = average_timeseries(fcst_timeseries)

            for i in range(len(avg_timeseries)):
                if float(avg_timeseries[i][1]) < 0:
                    avg_timeseries[i][1] = 0

            if avg_timeseries is not None and len(avg_timeseries)>0:
                Sim_TS.insert_data(timeseries=avg_timeseries, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred while updating fcst rainfalls in curw_sim.")
    finally:
        destroy_Pool(curw_sim_pool)
        destroy_Pool(curw_fcst_pool)


# for bulk insertion for a given one grid interpolation method
def update_rainfall_obs(flo2d_model, method, grid_interpolation, timestep):

    """
    Update rainfall observations for flo2d models
    :param flo2d_model: flo2d model
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :param timestep: output timeseries timestep
    :return:
    """

    now = datetime.now()
    OBS_START_STRING = (now - timedelta(days=10)).strftime('%Y-%m-%d %H:00:00')
    OBS_START = datetime.strptime(OBS_START_STRING, '%Y-%m-%d %H:%M:%S')

    try:

        # Connect to the database
        curw_obs_pool = get_Pool(host=CURW_OBS_HOST, user=CURW_OBS_USERNAME, password=CURW_OBS_PASSWORD,
                                 port=CURW_OBS_PORT, db=CURW_OBS_DATABASE)

        curw_obs_connection = curw_obs_pool.connection()

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                                 port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        # test ######
        # pool = get_Pool(host=HOST, user=USERNAME, password=PASSWORD, port=PORT, db=DATABASE)

        TS = Sim_Timeseries(pool=curw_sim_pool)

        # [hash_id, station_id, station_name, latitude, longitude]
        active_obs_stations = read_csv('grids/obs_stations/rainfall/curw_active_rainfall_obs_stations.csv')
        flo2d_grids = read_csv('grids/flo2d/{}m.csv'.format(flo2d_model))  # [Grid_ ID, X(longitude), Y(latitude)]

        stations_dict_for_obs = { }  # keys: obs station id , value: hash id

        for obs_index in range(len(active_obs_stations)):
            stations_dict_for_obs[active_obs_stations[obs_index][1]] = active_obs_stations[obs_index][0]

        flo2d_obs_mapping = get_flo2d_cells_to_obs_grid_mappings(pool=curw_sim_pool, grid_interpolation=grid_interpolation, flo2d_model=flo2d_model)

        for flo2d_index in range(len(flo2d_grids)):
            obs_start = OBS_START
            lat = flo2d_grids[flo2d_index][2]
            lon = flo2d_grids[flo2d_index][1]
            cell_id = flo2d_grids[flo2d_index][0]
            meta_data = {
                    'latitude': float('%.6f' % float(lat)), 'longitude': float('%.6f' % float(lon)),
                    'model': flo2d_model, 'method': method,
                    'grid_id': '{}_{}_{}'.format(flo2d_model, grid_interpolation, (str(cell_id)).zfill(10))
                    }

            tms_id = TS.get_timeseries_id(grid_id=meta_data.get('grid_id'), method=meta_data.get('method'))

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                TS.insert_run(meta_data=meta_data)

            obs_end = TS.get_obs_end(id_=tms_id)

            if obs_end is not None:
                obs_start = obs_end - timedelta(hours=1)

            obs1_station_id = str(flo2d_obs_mapping.get(meta_data['grid_id'])[0])
            obs2_station_id = str(flo2d_obs_mapping.get(meta_data['grid_id'])[1])
            obs3_station_id = str(flo2d_obs_mapping.get(meta_data['grid_id'])[2])

            obs_timeseries = []

            if timestep == 5:
                if obs1_station_id != str(-1):
                    obs1_hash_id = stations_dict_for_obs.get(obs1_station_id)

                    ts = extract_obs_rain_5_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs1_hash_id)
                    if ts is not None and len(ts) > 1:
                        obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts, expected_start=obs_start)[1:])
                        # obs_start = ts[-1][0]

                    if obs2_station_id != str(-1):
                        obs2_hash_id = stations_dict_for_obs.get(obs2_station_id)

                        ts2 = extract_obs_rain_5_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs2_hash_id)
                        if ts2 is not None and len(ts2) > 1:
                            obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts2, OBS_TS=obs_timeseries)
                            if obs_timeseries is not None and len(obs_timeseries) > 0:
                                expected_start = obs_timeseries[-1][0]
                            else:
                                expected_start= obs_start
                            obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts2, expected_start=expected_start)[1:])
                            # obs_start = ts2[-1][0]

                        if obs3_station_id != str(-1):
                            obs3_hash_id = stations_dict_for_obs.get(obs3_station_id)

                            ts3 = extract_obs_rain_5_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs3_hash_id)
                            if ts3 is not None and len(ts3) > 1 and len(obs_timeseries) > 0:
                                obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts3, OBS_TS=obs_timeseries)
                                if obs_timeseries is not None:
                                    expected_start = obs_timeseries[-1][0]
                                else:
                                    expected_start= obs_start
                                obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts3, expected_start=expected_start)[1:])
            elif timestep == 15:
                if obs1_station_id != str(-1):
                    obs1_hash_id = stations_dict_for_obs.get(obs1_station_id)

                    ts = extract_obs_rain_15_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs1_hash_id)
                    if ts is not None and len(ts) > 1:
                        obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts, expected_start=obs_start)[1:])
                        # obs_start = ts[-1][0]

                    if obs2_station_id != str(-1):
                        obs2_hash_id = stations_dict_for_obs.get(obs2_station_id)

                        ts2 = extract_obs_rain_15_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs2_hash_id)
                        if ts2 is not None and len(ts2) > 1:
                            obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts2, OBS_TS=obs_timeseries)
                            if obs_timeseries is not None and len(obs_timeseries) > 0:
                                expected_start = obs_timeseries[-1][0]
                            else:
                                expected_start = obs_start
                            obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts2, expected_start=expected_start)[1:])
                            # obs_start = ts2[-1][0]

                        if obs3_station_id != str(-1):
                            obs3_hash_id = stations_dict_for_obs.get(obs3_station_id)

                            ts3 = extract_obs_rain_15_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs3_hash_id)
                            if ts3 is not None and len(ts3) > 1 and len(obs_timeseries) > 0:
                                obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts3, OBS_TS=obs_timeseries)
                                if obs_timeseries is not None:
                                    expected_start = obs_timeseries[-1][0]
                                else:
                                    expected_start = obs_start
                                obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts3, expected_start=expected_start)[1:])

            for i in range(len(obs_timeseries)):
                if obs_timeseries[i][1] == -99999:
                    obs_timeseries[i][1] = 0

            if obs_timeseries is not None and len(obs_timeseries) > 0:
                TS.insert_data(timeseries=obs_timeseries, tms_id=tms_id, upsert=True)
                TS.update_latest_obs(id_=tms_id, obs_end=(obs_timeseries[-1][1]))

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred while updating obs rainfalls in curw_sim.")
    finally:
        curw_obs_connection.close()
        destroy_Pool(pool=curw_sim_pool)
        destroy_Pool(pool=curw_obs_pool)
        logger.info("Process finished")


