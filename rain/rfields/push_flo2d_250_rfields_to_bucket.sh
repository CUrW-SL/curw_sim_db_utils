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


# Generate flo2d 250 rfields in obs range
echo "Running push_flo2d_250_rfields_to_bucket.py"
python rain/rfields/push_flo2d_250_rfields_to_bucket.py >> rain/rfields/curw_sim_push_flo2d_250_rfields_to_bucket.log 2>&1

# Deactivating virtual environment
echo "Deactivating virtual environment"
deactivate
