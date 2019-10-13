import traceback
import json
from datetime import datetime, timedelta

from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.constants import FLO2D_250
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum

import sys
sys.path.insert(0, '/home/uwcc-admin/curw_sim_db_utils/rain/flo2d')
import flo2d_common

FLO2D_250_RFIELD_DIR = "rain/rfields/flo2d_250"

"""
if a date is not specified in the config:
    generate hourly obs rfields (for latest 2 hour period)
else:
    generate all rfields for the specified day
"""


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

        config = json.loads(open('rain/rfields/250_config.json').read())

        # date
        date = read_attribute_from_config_file('model_list', config, False)

        # source details
        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        if date:
            config_date = datetime.strptime(date, "%Y-%m-%d")
            start = config_date.strftime("%Y-%m-%d 00:00:00")
            end = config_date.strftime("%Y-%m-%d 23:55:00")
        else:
            now_sl = (datetime.now() + timedelta(hours=5, minutes=30))
            start = (now_sl - timedelta(hours=2)).strftime("%Y-%m-%d %H:00:00")
            end = now_sl.strftime("%Y-%m-%d %H:%M:00")

        print("{} : ####### Generate FLO2D 250 rfields".format(datetime.now()))
        flo2d_common.prepare_rfields(root_dir=FLO2D_250_RFIELD_DIR, start_time=start, end_time=end,
                                     target_model=FLO2D_250)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### Rfield generation process finished for {} #######".format(datetime.now(), FLO2D_250))



