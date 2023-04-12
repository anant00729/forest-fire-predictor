from shapely.geometry import Polygon


def convert_coords_to_plotable(row):
    lat_point_list = list(row['lat'].values)
    lon_point_list = list(row['long'].values)
    return Polygon(zip(lon_point_list, lat_point_list))