files_to_read_dict = {
    "CO 1980 - 2008": "CO_1980_2008_cleaned_stats.csv",
    "CO 2009 - 2020": "CO_2009_2020_cleaned_stats.csv",
    "NO2 1998 - 2008": "NO2_1980_2008_cleaned_stats.csv",
    "NO2 2009 - 2020": "NO2_2009_2020_cleaned_stats.csv",
    "NO 1980 - 2008": "NO_1980_2008_cleaned_stats.csv",
    "NO 2009 - 2020": "NO_2009_2020_cleaned_stats.csv",
    "O3 1980 - 2008": "O3_1980_2008_cleaned_stats.csv",
    "O3 2009 - 2020": "O3_2009_2020_cleaned_stats.csv",
    "PM10 1980 - 2008": "PM10_1980_2008_cleaned_stats.csv",
    "PM10 2009 - 2020": "PM10_2009_2020_cleaned_stats.csv",
    "PM25 1980 - 2008": "PM25_1980_2008_cleaned_stats.csv",
    "PM25 2009 - 2020": "PM25_2009_2020_cleaned_stats.csv",
    "SO 1980 - 2008": "SO2_1980_2008_cleaned_stats.csv",
    "SO 2009 - 2020": "SO2_2009_2020_cleaned_stats.csv"
}

file_base_path = 'apps/rka73/data/spark_transformed_files_with_geo_coords/'
files_to_read_dropdown = [
    {"label": "CO 1980 - 2008", "value": f"{file_base_path}CO_1980_2008_cleaned_stats.csv"},
    {"label": "CO 2009 - 2020", "value": f"{file_base_path}CO_2009_2020_cleaned_stats.csv"},
    {"label": "NO2 1998 - 2008", "value": f"{file_base_path}NO2_1980_2008_cleaned_stats.csv"},
    {"label": "NO2 2009 - 2020", "value": f"{file_base_path}NO2_2009_2020_cleaned_stats.csv"},
    {"label": "NO 1980 - 2008", "value": f"{file_base_path}NO_1980_2008_cleaned_stats.csv"},
    {"label": "NO 2009 - 2020", "value": f"{file_base_path}NO_2009_2020_cleaned_stats.csv"},
    {"label": "O3 1980 - 2008", "value": f"{file_base_path}O3_1980_2008_cleaned_stats.csv"},
    {"label": "O3 2009 - 2020", "value": f"{file_base_path}O3_2009_2020_cleaned_stats.csv"},
    {"label": "PM10 1980 - 2008", "value": f"{file_base_path}PM10_1980_2008_cleaned_stats.csv"},
    {"label": "PM10 2009 - 2020", "value": f"{file_base_path}PM10_2009_2020_cleaned_stats.csv"},
    {"label": "PM25 1980 - 2008", "value": f"{file_base_path}PM25_1980_2008_cleaned_stats.csv"},
    {"label": "PM25 2009 - 2020", "value": f"{file_base_path}PM25_2009_2020_cleaned_stats.csv"},
    {"label": "SO2 1980 - 2008", "value": f"{file_base_path}SO2_1980_2008_cleaned_stats.csv"},
    {"label": "SO2 2009 - 2020", "value": f"{file_base_path}SO2_2009_2020_cleaned_stats.csv"}
]

months = ['January',
          'February',
          'March',
          'April',
          'May',
          'June',
          'July',
          'August',
          'September',
          'October',
          'November',
          'December']

colors_for_hr_avg = {
    'yellow': '#FBBF24',
    'green': '#4ec66c',
    'red': '#ff728f',
}

files_to_read_dropdown_for_api = [
    {"label": "CO 1980 - 2008", "value": "CO_1980_2008_stats"},
    {"label": "CO 2009 - 2020", "value": "CO_2009_2020_stats"},
    {"label": "NO2 1998 - 2008", "value": "NO2_1980_2008_stats"},
    {"label": "NO2 2009 - 2020", "value": "NO2_2009_2020_stats"},
    {"label": "NO 1980 - 2008", "value": "NO_1980_2008_stats"},
    {"label": "NO 2009 - 2020", "value": "NO_2009_2020_stats"},
    {"label": "O3 1980 - 2008", "value": "O3_1980_2008_stats"},
    {"label": "O3 2009 - 2020", "value": "O3_2009_2020_stats"},
    {"label": "PM10 1980 - 2008", "value": "PM10_1980_2008_stats"},
    {"label": "PM10 2009 - 2020", "value": "PM10_2009_2020_stats"},
    {"label": "PM25 1980 - 2008", "value": "PM25_1980_2008_stats"},
    {"label": "PM25 2009 - 2020", "value": "PM25_2009_2020_stats"},
    {"label": "SO2 1980 - 2008", "value": "SO2_1980_2008_stats"},
    {"label": "SO2 2009 - 2020", "value": "SO2_2009_2020_stats"}
]


def get_tname_from_filename(filename):
    res = next(filter(lambda x: x['value'] == filename, files_to_read_dropdown), None)
    return next(filter(lambda x: x['label'] == res['label'], files_to_read_dropdown_for_api), None)['value']
