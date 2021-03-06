import traceback
import json
from datetime import datetime, timedelta

from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.constants import FLO2D_150
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum

import sys
sys.path.insert(0, '/home/uwcc-admin/curw_sim_db_utils/rain/flo2d')
import flo2d_common

FLO2D_150_RFIELD_DIR = "rain/rfields/flo2d_150"


def read_attribute_from_config_file(attribute, config, compulsory):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :param compulsory: Boolean value: whether the attribute is must present or not in the config file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    elif compulsory:
        print("{} not specified in config file.".format(attribute))
        exit(1)
    else:
        return None


if __name__=="__main__":
    try:

        config = json.loads(open('rain/rfields/150_config.json').read())

        # date
        date = read_attribute_from_config_file('day', config, True)

        # source details
        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        config_date = datetime.strptime(date, "%Y-%m-%d")
        start = config_date.strftime("%Y-%m-%d 00:00:00")
        end = config_date.strftime("%Y-%m-%d 23:55:00")

        print("{} : ####### Generate FLO2D 150 rfields".format(datetime.now()))
        flo2d_common.prepare_rfields(root_dir=FLO2D_150_RFIELD_DIR, start_time=start, end_time=end,
                                     target_model=FLO2D_150)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### Rfield generation process finished for {} #######".format(datetime.now(), FLO2D_150))



