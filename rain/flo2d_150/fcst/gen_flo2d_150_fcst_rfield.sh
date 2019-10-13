#!/usr/bin/env bash

echo `date`

echo "Changing into ~/curw_sim_db_utils"
cd /home/uwcc-admin/curw_sim_db_utils
echo "Inside `pwd`"


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


# Generate flo2d 150 rfields in fcst range
echo "Running gen_flo2d_150_fcst_rfield.py"
python rain/flo2d_150/fcst/gen_flo2d_150_fcst_rfield.py >> rain/flo2d_150/fcst/curw_sim_rfield_fcst_150.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
