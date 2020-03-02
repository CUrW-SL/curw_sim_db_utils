#
#!/home/uwcc-admin/curw_sim_db_utils/venv/bin/python3
import traceback
import pandas as pd
from datetime import datetime, timedelta
import os, sys, getopt

import numpy as np
import geopandas as gpd
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point


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
    -s  --shape_file    Path to the shape file containing the whole area. Default is "PROJECT_ROOT/regions/kelani_basin_hec_wgs/kelani_basin_hec_wgs.shp"
    """
    print(usageText)


def _voronoi_finite_polygons_2d(vor, radius=None):
    """
    Reconstruct infinite voronoi regions in a 2D diagram to finite
    regions.
    Parameters
    ----------
    vor : Voronoi
        Input diagram
    radius : float, optional
        Distance to 'points at infinity'.
    Returns
    -------
    regions : list of tuples
        Indices of vertices in each revised Voronoi regions.
    vertices : list of tuples
        Coordinates for revised Voronoi vertices. Same as coordinates
        of input vertices, with 'points at infinity' appended to the
        end.
    from: https://stackoverflow.com/questions/20515554/colorize-voronoi-diagram
    """
    if vor.points.shape[1] != 2:
        raise ValueError("Requires 2D input")

    new_regions = []
    new_vertices = vor.vertices.tolist()
    center = vor.points.mean(axis=0)
    if radius is None:
        radius = vor.points.ptp().max()
    # Construct a map containing all ridges for a given point
    all_ridges = {}
    for (p1, p2), (v1, v2) in zip(vor.ridge_points, vor.ridge_vertices):
        all_ridges.setdefault(p1, []).append((p2, v1, v2))
        all_ridges.setdefault(p2, []).append((p1, v1, v2))

    # Reconstruct infinite regions
    for p1, region in enumerate(vor.point_region):
        vertices = vor.regions[region]
        if all(v >= 0 for v in vertices):
            # finite region
            new_regions.append(vertices)
            continue
        # reconstruct a non-finite region
        ridges = all_ridges[p1]
        new_region = [v for v in vertices if v >= 0]

        for p2, v1, v2 in ridges:
            if v2 < 0:
                v1, v2 = v2, v1
            if v1 >= 0:
                # finite ridge: already in the region
                continue
            # Compute the missing endpoint of an infinite ridge
            t = vor.points[p2] - vor.points[p1]  # tangent
            t /= np.linalg.norm(t)
            n = np.array([-t[1], t[0]])  # normal

            midpoint = vor.points[[p1, p2]].mean(axis=0)
            direction = np.sign(np.dot(midpoint - center, n)) * n
            far_point = vor.vertices[v2] + direction * radius
            new_region.append(len(new_vertices))
            new_vertices.append(far_point.tolist())

        # sort region counterclockwise
        vs = np.asarray([new_vertices[v] for v in new_region])
        c = vs.mean(axis=0)
        angles = np.arctan2(vs[:, 1] - c[1], vs[:, 0] - c[0])
        new_region = np.array(new_region)[np.argsort(angles)]
        # finish
        new_regions.append(new_region.tolist())
    return new_regions, np.asarray(new_vertices)


def get_voronoi_polygons(points_dict, shape_file, shape_attribute=None, output_shape_file=None, add_total_area=True):
    """
    :param points_dict: dict of points {'id' --> [lon, lat]}
    :param shape_file: shape file path of the area
    :param shape_attribute: attribute list of the interested region [key, value]
    :param output_shape_file: if not none, a shape file will be created with the output
    :param add_total_area: if true, total area shape will also be added to output
    :return:
    geo_dataframe with voronoi polygons with columns ['id', 'lon', 'lat','area', 'geometry'] with last row being the area of the
    shape file
    """
    if shape_attribute is None:
        shape_attribute = ['OBJECTID', 1]

    shape_df = gpd.GeoDataFrame.from_file(shape_file)
    shape_polygon_idx = shape_df.index[shape_df[shape_attribute[0]] == shape_attribute[1]][0]
    shape_polygon = shape_df['geometry'][shape_polygon_idx]

    ids = [p if type(p) == str else np.asscalar(p) for p in points_dict.keys()]
    points = np.array(list(points_dict.values()))[:, :2]
    vor = Voronoi(points)

    regions, vertices = _voronoi_finite_polygons_2d(vor)

    data = []
    for i, region in enumerate(regions):
        polygon = Polygon([tuple(x) for x in vertices[region]])
        if polygon.intersects(shape_polygon):
            intersection = polygon.intersection(shape_polygon)
            data.append({'id': ids[i], 'lon': vor.points[i][0], 'lat': vor.points[i][1], 'area': intersection.area,
                         'geometry': intersection
                         })
    df = gpd.GeoDataFrame(data, columns=['id', 'lon', 'lat', 'area', 'geometry'], crs=shape_df.crs)
    if output_shape_file is not None:
        df.to_file(output_shape_file)

    return df


if __name__=="__main__":

    ROOT_DIRECTORY = "/home/shadhini/dev/repos/curw-sl/curw_sim_db_utils/_event_simulation"
    PROJECT_DIR = "/home/shadhini/dev/repos/curw-sl/curw_sim_db_utils"
    set_db_config_file_path(os.path.join(ROOT_DIRECTORY, 'db_adapter_config.json'))

    try:

        flo2d_model = "flo2d_250"
        run_time = (datetime.now()).strftime("%Y-%m-%d 00:00:00")
        file_path = None
        shape_file_path = None
        file_path_part1 = '/mnt/disks/wrf_nfs/wrf/4.1.2/d0/18'
        file_path_part2 = 'rfields/wrf/d03_kelani_basin/corrected_rf.csv'

        try:
            opts, args = getopt.getopt(sys.argv[1:], "h:m:r:f:s:",
                    ["help", "flo2d_model=", "run_time=", "file_path=", "shape_file="])
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
            elif opt in ("-s", "--shape_file"):
                shape_file_path = arg.strip()

        if flo2d_model not in ("flo2d_250", "flo2d_150"):
            print("Flo2d model should be either \"flo2d_250\" or \"flo2d_150\"")
            exit(1)

        if file_path is None:
            file_path = os.path.join(file_path_part1, run_time.split(' ')[0], file_path_part2)

        if shape_file_path is None:
            shape_file_path = os.path.join(PROJECT_DIR, 'regions/kelani_basin_hec_wgs/kelani_basin_hec_wgs.shp')

        corrected_rf_df = pd.read_csv(file_path, delimiter=',')
        distinct_stations = corrected_rf_df.groupby(['longitude', 'latitude']).size()

        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.TP)

        # [Grid_ ID, X(longitude), Y(latitude)]
        flo2d_grids = read_csv(os.path.join(PROJECT_DIR, 'grids/flo2d/{}m.csv'.format(flo2d_model)))

        pool = get_Pool(host=con_params.CURW_SIM_HOST, port=con_params.CURW_SIM_PORT,
                        db=con_params.CURW_SIM_DATABASE,
                        user=con_params.CURW_SIM_USERNAME, password=con_params.CURW_SIM_PASSWORD)

        points_dict = {}
        print('distinct_stations')
        count = 1000
        for index, row in distinct_stations.iteritems():
            points_dict['point_{}'.format(count)] = [index[0], index[1]]
            count += 1

        output_shape_file_path = os.path.join(PROJECT_DIR, 'regions/output', "{}_out_shp.shp".format((datetime.now()).strftime("%Y-%m-%d_%H-%M-%S")))

        get_voronoi_polygons(points_dict=points_dict, shape_file=shape_file_path, shape_attribute=None,
                             output_shape_file=output_shape_file_path, add_total_area=True)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("Process finished.")


