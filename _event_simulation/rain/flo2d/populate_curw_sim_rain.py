#
#!/home/uwcc-admin/curw_sim_db_utils/venv/bin/python3
import traceback
import pandas as pd
from datetime import datetime, timedelta
import os, sys, getopt

from db_adapter.base import get_Pool
from db_adapter.constants import set_db_config_file_path
from db_adapter.constants import connection as con_params
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_sim.grids import GridInterpolationEnum

from db_adapter.csv_utils import read_csv
from db_adapter.logger import logger


def usage():
    usageText = """
    -----------------------------------------------------
    Populate curw sim rain for Flo2D 250 & 150 Raincells
    -----------------------------------------------------

    Usage: ./populate_curw_sim_rain.py [-m flo2d_XXX] [-r "YYYY-MM-DD HH:MM:SS"] [-f "file_path"]

    -h  --help          Show usage
    -m  --flo2d_model   FLO2D model (e.g. flo2d_250, flo2d_150). Default is flo2d_250.
    -r  --run_time      Event simulation run time (e.g: "2019-06-05 23:30:00"). Default is 00:00:00, today.
    -f  --file_path     Corrected rain file path. e.g.: "/mnt/disks/wrf_nfs/wrf/corrected_rf.csv"
    """
    print(usageText)


if __name__=="__main__":

    ROOT_DIRECTORY = "/home/shadhini/dev/repos/curw-sl/curw_sim_db_utils/_event_simulation"
    PROJECT_DIR = "/home/shadhini/dev/repos/curw-sl/curw_sim_db_utils"
    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        flo2d_model = "flo2d_250"
        run_time = (datetime.now()).strftime("%Y-%m-%d 00:00:00")
        file_path = None
        file_path_part1 = '/mnt/disks/wrf_nfs/wrf/4.1.2/d0/18'
        file_path_part2 = 'rfields/wrf/d03_kelani_basin/corrected_rf.csv'

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:r:f:",
                    ["help", "flo2d_model=", "run_time=", "file_path="])
        except getopt.GetoptError:
            usage()
            sys.exit(2)
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-m", "--flo2d_model"):
                flo2d_model = arg.strip()
            elif opt in ("-r", "--run_time"):
                run_time = arg.strip()
            elif opt in ("-f", "--file_path"):
                file_path = arg.strip()

        if flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        if file_path is None:
            file_path = os.path.join(file_path_part1, run_time.split(' ')[0], file_path_part2)

        corrected_rf_df = pd.read_csv(file_path, delimiter=',')
        distinct_stations = corrected_rf_df.groupby(['longitude', 'latitude']).size()

        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.TP)

        # [Grid_ ID, X(longitude), Y(latitude)]
        flo2d_grids = read_csv(os.path.join(PROJECT_DIR, 'grids/flo2d/{}m.csv'.format(flo2d_model)))

        pool = get_Pool(host=con_params.CURW_SIM_HOST, port=con_params.CURW_SIM_PORT,
                        db=con_params.CURW_SIM_DATABASE,
                        user=con_params.CURW_SIM_USERNAME, password=con_params.CURW_SIM_PASSWORD)

        print(corrected_rf_df)
        print('distinct_stations')
        print(distinct_stations.size)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("Process finished.")
