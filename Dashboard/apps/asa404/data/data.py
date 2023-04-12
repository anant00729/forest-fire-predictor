all_station_location = [{"station_name" : "Northwest Fire Centre", "lat" : 56.56036501074665, "long" : -127.86863015078664, "zoom" : 5},
        {"station_name" : "Coastal Fire Centre", "lat" : 51.76933359471333, "long" : -125.1057852707031, "zoom" : 5},
        {"station_name" : "Prince George Fire Centre", "lat" :55.866330253094006, "long" : -124.84749685136535, "zoom" : 4.5},
        {"station_name" : "Cariboo Fire Centre", "lat" : 52.221574946135085, "long" : -122.64695148815353, "zoom" : 6},
        {"station_name" : "Kamloops Fire Centre", "lat" : 50.98176325901955, "long" : -120.76191592799093, "zoom" : 6},
        {"station_name" : "Southeast Fire Centre", "lat" : 51.02748740772872, "long" : -117.01162952139136, "zoom" : 6}]


def get_location_from_station_name(station_name):
  return next(filter(lambda x: x['station_name'] == station_name, all_station_location), None)
