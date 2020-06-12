#!/usr/bin/env bash

echo `date`

#FILE_MODIFIED_TIME=$(date -r /home/uwcc-admin/curw_sim_db_utils/rain/config.json +%s)
FILE_MODIFIED_TIME=$(date -r /home/uwcc-admin/wrf_rfields/4.1.2/d0/18/rfields/wrf/d03_kelani_basinconfig.json.txt +%s)
CURRENT=$(date +%s)

DIFF=$(((CURRENT-FILE_MODIFIED_TIME)/60))
echo $DIFF


if [ $DIFF -lt 370 ]
then
  echo "New fcst rain config !!!"
  echo "Changing into ~/curw_sim_db_utils"
  cd /home/uwcc-admin/curw_sim_db_utils
  echo "Inside `pwd`"

  cp /home/uwcc-admin/wrf_rfields/4.1.2/d0/18/rfields/wrf/d03_kelani_basinconfig.json.txt rain/config.json

  # If no venv (python3 virtual environment) exists, then create one.
  if [ ! -d "venv" ]
  then
      echo "Creating venv python3 virtual environment."
      virtualenv -p python3 venv
  fi

  # Activate venv.
  echo "Activating venv python3 virtual environment."
  source venv/bin/activate

  # Install dependencies using pip.
  if [ ! -f "curw_sim_utils.log" ]
  then
      echo "Installing PyMySQL"
      pip install PyMySQL
      echo "Installing PyYAML"
      pip install PyYAML
      echo "Installing db adapter"
      pip install git+https://github.com/shadhini/curw_db_adapter.git
      touch curw_sim_utils.log
  fi

  # Update obs data in curw_sim for flo2d grids
  echo "Running update_active_curw_rainfall_stations.py"
  python grids/obs_stations/rainfall/update_active_curw_rainfall_stations.py >> grids/obs_stations/rainfall/curw_sim_update_active_curw_rainfall_stations.log 2>&1

  # Update fcst data in curw_sim for hechms grids
  echo "Running update_fcst_rainfall.py"
  python rain/hechms/fcst/update_fcst_rainfall.py >> rain/hechms/fcst/curw_sim_fcst_hechms.log 2>&1

  # Update fcst data in curw_sim for flo2d 250 grids
  echo "Running update_fcst_rainfall_flo2d_250.py"
  python rain/flo2d_250/fcst/update_fcst_rainfall_flo2d_250.py >> rain/flo2d_250/fcst/curw_sim_fcst_flo2d_250.log 2>&1

  # Update fcst data in curw_sim for flo2d 150 grids
  echo "Running update_fcst_rainfall_flo2d_150.py"
  python rain/flo2d_150/fcst/update_fcst_rainfall_flo2d_150.py >> rain/flo2d_150/fcst/curw_sim_fcst_flo2d_150.log 2>&1

  # Update fcst data in curw_sim for flo2d 150 grids
  echo "Running update_fcst_rainfall_flo2d_150_v2.py"
  python rain/flo2d_150_v2/fcst/update_fcst_rainfall_flo2d_150_v2.py >> rain/flo2d_150_v2/fcst/curw_sim_fcst_flo2d_150_v2.log 2>&1

  # Generate flo2d 250 rfields in fcst range
  echo "Running gen_flo2d_250_fcst_rfield.py"
  python rain/flo2d_250/fcst/gen_flo2d_250_fcst_rfield.py >> rain/flo2d_250/fcst/curw_sim_rfield_fcst_250.log 2>&1

  # Generate flo2d 150 rfields in fcst range
  echo "Running gen_flo2d_150_fcst_rfield.py"
  python rain/flo2d_150/fcst/gen_flo2d_150_fcst_rfield.py >> rain/flo2d_150/fcst/curw_sim_rfield_fcst_150.log 2>&1


  # Deactivating virtual environment
  echo "Deactivating virtual environment"
  deactivate

fi



