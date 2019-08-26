import csv
import operator
import collections
import traceback

from math import acos, cos, sin, radians


def create_csv(file_name, data):
    """
    Create new csv file using given data
    :param file_name: <file_path/file_name>.csv
    :param data: list of lists
    e.g. [['Person', 'Age'], ['Peter', '22'], ['Jasmine', '21'], ['Sam', '24']]
    :return:
    """
    with open(file_name, 'w') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerows(data)


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][1:]

    return data


def find_nearest_obs_stations_for_flo2d_stations(flo2d_stations_csv, obs_stations_csv, flo2d_model):

    obs_stations = read_csv(obs_stations_csv)  # [hash_id,station_id,station_name,latitude,longitude]

    flo2d_station = read_csv(flo2d_stations_csv)  # [Grid_ID,X,Y]

    flo2d_obs_mapping_list = [['{}_station_id'.format(flo2d_model), 'ob_1_id', 'ob_1_dist', 'ob_2_id', 'ob_2_dist', 'ob_3_id',
                               'ob_3_dist']]

    for flo2d_index in range(len(flo2d_station)):

        flo2d_obs_mapping = [flo2d_station[flo2d_index][0]]

        flo2d_lat = float(flo2d_station[flo2d_index][2])
        flo2d_lng = float(flo2d_station[flo2d_index][1])

        distances = {}

        for obs_index in range(len(obs_stations)):
            lat = float(obs_stations[obs_index][3])
            lng = float(obs_stations[obs_index][4])

            intermediate_value = cos(radians(flo2d_lat)) * cos(radians(lat)) * cos(
                    radians(lng) - radians(flo2d_lng)) + sin(radians(flo2d_lat)) * sin(radians(lat))
            if intermediate_value < 1:
                distance = 6371 * acos(intermediate_value)
            else:
                distance = 6371 * acos(1)

            distances[obs_stations[obs_index][2]] = distance

        sorted_distances = collections.OrderedDict(sorted(distances.items(), key=operator.itemgetter(1))[:10])

        for key in sorted_distances.keys():
            flo2d_obs_mapping.extend([key, sorted_distances.get(key)])

        print(flo2d_obs_mapping)
        flo2d_obs_mapping_list.append(flo2d_obs_mapping)

    create_csv('MDPA_{}_obs_mapping.csv'.format(flo2d_model), flo2d_obs_mapping_list)


# find_nearest_obs_stations_for_flo2d_stations('flo2d_30m.csv', 'curw_active_rainfall_obs_stations.csv')
