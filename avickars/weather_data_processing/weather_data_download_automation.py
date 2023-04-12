import sys
import time
from selenium import webdriver


def main():
    with open('ghcnd-stations.txt') as f:
        lines = f.readlines()

    slices = [(0, 11), (12, 20), (21, 30), (32, 37), (38, 40), (41, 71), (73, 75), (76, 79), (80, 85)]

    browser = webdriver.Chrome("chromedriver.exe")
    for line in lines:
        lineSplit = [line[slice(*slc)] for slc in slices]
        if lineSplit[4] == 'BC':
            print(lineSplit)
            browser.get(url=f"""https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_station/{lineSplit[0]}.csv.gz""")
            time.sleep(2)


if __name__ == '__main__':
    main()
