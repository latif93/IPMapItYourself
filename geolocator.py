import glob
import json
import requests
import numpy as np
import pprint
import re
import pandas as pd
import scipy.constants as constant
import time
from constants import DF_COLS
from ripe_atlas_client import RIPEAtlasClient
from ripe.atlas.cousteau import AtlasResultsRequest
from pdbutils import PeeringDB, Location

class Geolocator:
    def __init__(self, ripe_atlas_client=None):
        self.ripe_atlas_client = ripe_atlas_client
        with open("static/coords.json", "r") as f:
            self.coords = json.load(f)

    def get_loc_ripe_atlas(self, addr, results):
        measurements = list() 
        for msm in results:
            avg_rtt = msm['avg'] 
            if avg_rtt == -1:
                continue
            avg_one_way = avg_rtt / 2

            if avg_one_way < 10: 
                measurements.append(msm)
        measurements = sorted(measurements, key=lambda x:x['avg'])
        
        if len(measurements) == 0:
            print(f'{addr} is un-pingable...')
            return ['NAN'] * 5

        lowest_rtt = measurements[0]['avg'] / 2 / 1000
        l_pid = measurements[0]['prb_id']
        p_lon, p_lat = self.ripe_atlas_client.PID_TO_RIPE_PROBE[l_pid]['geometry']['coordinates']
        print(f'Lowest one way RTT is {lowest_rtt*1000: .2f} ms')
        key = (round(p_lat, 4), round(p_lon, 4))
        key = str(key)

        if key not in self.coords:
            print(f'Getting loc for {key}')
            r = requests.get(f'https://ipmap-api.ripe.net/v1/worlds/reverse/{p_lat}/{p_lon}').json()
            self.coords[key] = r

        radius = lowest_rtt * (2/3) * (constant.speed_of_light / 1000) # m to km
        print(radius, 'Km')
        
        city = self.coords[key]['locations'][0]['cityNameAscii']
        c_code = self.coords[key]['locations'][0]['countryCodeAlpha2']
        country = self.coords[key]['locations'][0]['countryName']

        return city, c_code, country, p_lon, p_lat, l_pid

    def get_loc(self, addr, results):
        if self.ripe_atlas_client:
            return self.get_loc_ripe_atlas(addr, results)

if __name__ == '__main__':
    data = []
    ra_c = RIPEAtlasClient(api_key='380531a9-c3fb-424f-8d1b-23cda9b881fd')
    geolocator = Geolocator(ra_c)
    for m in glob.glob('artifacts/measurements.*.csv'):
        with open(m) as f:
            for line in f:
                try:
                    addr, m_id = line.strip().split(',')
                    m_id = int(m_id)
                except ValueError:
                    print(f'VALUEERROR: {line}')
                    continue  # Skip to the next iteration if there's an error parsing

                if m_id: 
                    is_success, results = AtlasResultsRequest(msm_id=m_id).create()
                    location_info = geolocator.get_loc(addr, results)
                    if location_info:
                        data.append((addr,) + location_info)
                    else:
                        continue  # Skip appending data if location info is None
    df = pd.DataFrame(data=data, columns=DF_COLS)
    df.to_csv("artifacts/results.csv")
