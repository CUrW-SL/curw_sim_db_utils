import traceback
from datetime import datetime, timedelta

from db_adapter.constants import COMMON_DATE_TIME_FORMAT
from db_adapter.curw_sim.constants import FLO2D_150
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum

import sys
sys.path.insert(0, '/home/uwcc-admin/curw_sim_db_utils/rain/flo2d')
import flo2d_common

FLO2D_150_RFIELD_DIR = "rain/rfields/flo2d_150"


if __name__=="__main__":
    try:

        # source details
        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        now_sl = (datetime.now() + timedelta(hours=5, minutes=30))
        start = (now_sl + timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00")
        end = (now_sl+timedelta(days=1)).strftime("%Y-%m-%d 23:30:00")

        print("{} : ####### Generate FLO2D 150 rfields".format(datetime.now()))
        flo2d_common.prepare_rfields(root_dir=FLO2D_150_RFIELD_DIR, start_time=start, end_time=end,
                                     target_model=FLO2D_150)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### Rfield generation process finished for {} #######".format(datetime.now(), FLO2D_150))



