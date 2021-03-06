import traceback
from datetime import datetime, timedelta
import json

from db_adapter.curw_sim.constants import FLO2D_250
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum
# from rain.flo2d.flo2d_common import update_rainfall_fcsts

import sys
sys.path.insert(0, '/home/uwcc-admin/curw_sim_db_utils/rain/flo2d')
import flo2d_common


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


if __name__=="__main__":
    try:

        config = json.loads(open('rain/config.json').read())

        # source details
        model_list = read_attribute_from_config_file('model_list', config)

        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        print("{} : ####### Insert fcst rainfall for FLO2D 250 grids".format(datetime.now()))
        flo2d_common.update_rainfall_fcsts(flo2d_model=FLO2D_250, method=method, grid_interpolation=grid_interpolation,
                model_list=model_list, timestep=5)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### fcst rainfall insertion process finished for {} #######".format(datetime.now(), FLO2D_250))



