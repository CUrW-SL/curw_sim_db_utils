import json
import traceback
from datetime import datetime, timedelta
from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_HOST
from db_adapter.curw_sim.grids import get_obs_to_d03_grid_mappings_for_rainfall
from db_adapter.curw_sim.constants import HecHMS
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_sim.timeseries import Timeseries as Sim_Timeseries
from db_adapter.curw_fcst.timeseries import Timeseries as Fcst_Timeseries
from db_adapter.curw_fcst.source import get_source_id
from db_adapter.curw_sim.common import convert_15_min_ts_to_5_mins_ts, append_value_for_timestamp, summed_timeseries

from db_adapter.logger import logger


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    else:
        print("{} not specified in config file.".format(attribute))
        exit(1)


# for bulk insertion for a given one grid interpolation method
def update_rainfall_fcsts(target_model, method, grid_interpolation, model_list, timestep):

    """
    Update rainfall forecasts for flo2d models
    :param target_model: target model for which input ins prepared
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :param model_list: list of forecast model and their versions used to calculate the rainfall
    e.g.: [["WRF_E", "4.0", "evening_18hrs"],["WRF_SE", "v4", ,"evening_18hrs"],["WRF_Ensemble", "4.0", ,"MME"]]
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

        # [hash_id, station_id, station_name, latitude, longitude]
        active_obs_stations = read_csv('grids/obs_stations/rainfall/curw_active_rainfall_obs_stations.csv')
        obs_stations_dict = { }  # keys: obs station id , value: [name, latitude, longitude]

        for obs_index in range(len(active_obs_stations)):
            obs_stations_dict[active_obs_stations[obs_index][1]] = [active_obs_stations[obs_index][2],
                                                                    active_obs_stations[obs_index][3],
                                                                    active_obs_stations[obs_index][4]]

        obs_d03_mapping = get_obs_to_d03_grid_mappings_for_rainfall(pool=curw_sim_pool, grid_interpolation=grid_interpolation)

        for obs_id in obs_stations_dict.keys():
            meta_data = {
                    'latitude': float('%.6f' % float(obs_stations_dict.get(obs_id)[1])),
                    'longitude': float('%.6f' % float(obs_stations_dict.get(obs_id)[2])),
                    'model': target_model, 'method': method,
                    'grid_id': 'rainfall_{}_{}_{}'.format(obs_id, obs_stations_dict.get(obs_id)[0], grid_interpolation)
                    }

            tms_id = Sim_TS.get_timeseries_id_if_exists(meta_data=meta_data)

            if tms_id is None:
                tms_id = Sim_TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                Sim_TS.insert_run(meta_data=meta_data)

            obs_end = Sim_TS.get_obs_end(id_=tms_id)

            fcst_timeseries = []

            for i in range(len(model_list)):

                source_id = get_source_id(pool=curw_fcst_pool, model=model_list[i][0], version=model_list[i][1])
                sim_tag = model_list[i][2]
                coefficient = model_list[i][3]

                temp_timeseries = []

                if timestep == 5:
                    if obs_end is not None:
                        temp_timeseries = convert_15_min_ts_to_5_mins_ts(
                                newly_extracted_timeseries=Fcst_TS.get_latest_timeseries(
                                        sim_tag=sim_tag, station_id=obs_d03_mapping.get(meta_data['grid_id'])[0],
                                        start=obs_end,
                                        source_id=source_id, variable_id=1, unit_id=1))
                    else:
                        temp_timeseries = convert_15_min_ts_to_5_mins_ts(
                                newly_extracted_timeseries=Fcst_TS.get_latest_timeseries(
                                        sim_tag=sim_tag, station_id=obs_d03_mapping.get(meta_data['grid_id'])[0],
                                        source_id=source_id, variable_id=1, unit_id=1))
                elif timestep == 15:
                    if obs_end is not None:
                        temp_timeseries = Fcst_TS.get_latest_timeseries(
                                        sim_tag=sim_tag,
                                        station_id=obs_d03_mapping.get(meta_data['grid_id'])[0],
                                        start=obs_end,
                                        source_id=source_id, variable_id=1, unit_id=1)
                    else:
                        temp_timeseries = Fcst_TS.get_latest_timeseries(
                                        sim_tag=sim_tag,
                                        station_id=obs_d03_mapping.get(meta_data['grid_id'])[0],
                                        source_id=source_id, variable_id=1, unit_id=1)

                if coefficient != 1:
                    for j in range(len(temp_timeseries)):
                        temp_timeseries[j][1] = float(temp_timeseries[j][1]) * coefficient

                if i==0:
                    fcst_timeseries = temp_timeseries
                else:
                    fcst_timeseries = append_value_for_timestamp(existing_ts=fcst_timeseries,
                            new_ts=temp_timeseries)

            sum_timeseries = summed_timeseries(fcst_timeseries)

            for i in range(len(sum_timeseries)):
                if float(sum_timeseries[i][1]) < 0:
                    sum_timeseries[i][1] = 0

            if sum_timeseries is not None and len(sum_timeseries)>0:
                Sim_TS.insert_data(timeseries=sum_timeseries, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred while updating fcst rainfalls in curw_sim.")

    finally:
        destroy_Pool(curw_sim_pool)
        destroy_Pool(curw_fcst_pool)


if __name__=="__main__":
    try:
        config = json.loads(open('rain/config.json').read())

        # source details
        model_list = read_attribute_from_config_file('model_list', config)

        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        print("{} : ####### Insert fcst rainfall for Obs Stations grids #######".format(datetime.now()))
        update_rainfall_fcsts(target_model=HecHMS, method=method, grid_interpolation=grid_interpolation,
                model_list=model_list, timestep=5)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### fcst rainfall insertion process finished for {} #######".format(datetime.now(), HecHMS))


