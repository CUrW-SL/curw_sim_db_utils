import traceback
from datetime import datetime, timedelta

from db_adapter.curw_sim.constants import FLO2D_150_V2
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum

# from rain.flo2d.flo2d_common import update_rainfall_obs

import sys
sys.path.insert(0, '/home/uwcc-admin/curw_sim_db_utils/rain/flo2d')

import flo2d_common

if __name__=="__main__":
    try:
        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        print("{} : ####### Insert obs rainfall for FLO2D 150 V2 grids".format(datetime.now()))
        flo2d_common.update_rainfall_obs(flo2d_model=FLO2D_150_V2, method=method, grid_interpolation=grid_interpolation, timestep=15)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### obs rainfall insertion process finished for {} #######".format(datetime.now(), FLO2D_150_V2))
