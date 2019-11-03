import traceback
import os
import json
from datetime import datetime, timedelta, date

from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.constants import FLO2D_250
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum

FLO2D_250_RFIELD_DIR = "rain/rfields/flo2d_250"
FLO2D_250_RFIELD_BUCKET_DIR = "/mnt/disks/wrf_nfs/flo2d_raincells/250/rfield"
FORWARD = 1
BACKWARD = 3


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
        day = read_attribute_from_config_file('day', config, False)

        # source details
        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        if day:
            day_0 = (datetime.strptime(day, "%Y-%m-%d")).date()
        else:
            day_0 = (datetime.now() + timedelta(hours=5, minutes=30)).date()

        start = day_0 - timedelta(days=BACKWARD)
        end = start + timedelta(days=FORWARD)

        delta = end - start  # as timedelta

        rfield_locations = ""

        for i in range(delta.days + 1):
            day = start + timedelta(days=i)
            rfield_locations += " {}/{}/*".format(FLO2D_250_RFIELD_DIR, day.strftime("%Y-%m-%d"))

        print("{} : ####### Push FLO2D 250 rfields to google bucket".format(datetime.now()))
        os.system("tar -czf {}/{}.tar.gz -C{}".format(FLO2D_250_RFIELD_BUCKET_DIR,
                                                   day_0.strftime("%Y-%m-%d_%H-%M"), rfield_locations))

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### Push {} rfields to google bucket #######".format(datetime.now(), FLO2D_250))
