#!/home/uwcc-admin/curw_sim_db_utils/venv/bin/python3
import pandas as pd
import os

bucket_flo2d_raincell_home = "/mnt/disks/wrf_nfs/flo2d_raincells"

if __name__=="__main__":

    grids_150 = pd.read_csv('grids/flo2d/flo2d_150m.csv', delimiter=',')
    grids_250 = pd.read_csv('grids/flo2d/flo2d_250m.csv', delimiter=',')

    xy_150 = grids_150[['X', 'Y']]
    xy_250 = grids_250[['X', 'Y']]

    xy_250.to_csv(os.path.join(bucket_flo2d_raincell_home, '250', 'xy.csv'), header=False, index=False)
    xy_150.to_csv(os.path.join(bucket_flo2d_raincell_home, '150', 'xy.csv'), header=False, index=False)
