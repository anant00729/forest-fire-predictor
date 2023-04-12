# ref: https://stackoverflow.com/questions/37885798/how-to-calculate-the-midpoint-of-several-geolocations-in-python
import numpy as np
import numpy.linalg as lin

E = np.array([[0, 0, 1],
              [0, 1, 0],
              [-1, 0, 0]])


def lat_long2n_E(latitude, longitude):
    res = [np.sin(np.deg2rad(latitude)),
         np.sin(np.deg2rad(longitude)) * np.cos(np.deg2rad(latitude)),
         -np.cos(np.deg2rad(longitude)) * np.cos(np.deg2rad(latitude))]
    return np.dot(E.T, np.array(res))


def n_E2lat_long(n_E):
    n_E = np.dot(E, n_E)
    longitude = np.arctan2(n_E[1], -n_E[2]);
    equatorial_component = np.sqrt(n_E[1] ** 2 + n_E[2] ** 2)
    latitude = np.arctan2(n_E[0], equatorial_component)
    return np.rad2deg(latitude), np.rad2deg(longitude)


def average_cord(coords):
    res = []
    for lat, lon in coords:
        res.append(lat_long2n_E(lat, lon))
    res = np.array(res)
    m = np.mean(res, axis=0)
    m = m / lin.norm(m)
    return n_E2lat_long(m)
