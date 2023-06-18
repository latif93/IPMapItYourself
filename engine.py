from enum import Enum
from single_radius import SingleRadius
from geolocator import Geolocator
from ripe_atlas_client import RIPEAtlasClient
from pdbutils import PeeringDB
from evaluator import Evaluator
from ripe.atlas.cousteau import AtlasResultsRequest
import json
import time
import pandas
from constants import DF_COLS


from constants import EngineType




class Engine():
    """
    This class orchestrates geolocation for a given set of IP addresses
    """
    def __init__(self, engine_type, ips, api_key=None, validation=False):
        self.engine_type = engine_type
        self.api_key = api_key
        self.ips = ips
        if self.engine_type == EngineType.RIPE:
            assert self.api_key is not None
            client = RIPEAtlasClient(api_key)
        self.single_radius = SingleRadius(PeeringDB(), client)
        self.geolocator = Geolocator(client)
        self.results = []
        self.validation = validation
        
    
    def run(self):
        for ip in self.ips:
            self.single_radius.measure_addr(ip)

        while not self.single_radius.check_for_completion():
            print("Checking if all measurements are complete")
            time.sleep(5 * 60)

        for t_addr, m_id in self.single_radius.measurement_info:
            is_success, results = AtlasResultsRequest(msm_id=m_id).create()
            city, c_code, country, p_lon, p_lat = self.geolocator.get_loc(t_addr, results)
            self.results.append((t_addr, city, c_code, country, p_lon, p_lat))
        df = pandas.DataFrame(data=self.results, columns=DF_COLS)
        df.to_csv('results.csv')
        print('Wrote Results To File')

        if self.validation:
            evaluator = Evaluator(df)
            evaluator.evaluate()
            evaluator.plot()
        
        print('Done')


def main():
    ips = []
    with open("static/Combined.json","r") as f:
        for line in f.readlines():
            data = json.loads(line)
            ipv4 = data['ipv4']
            if ipv4 is not None:
                ips.append(data['ipv4'])
    print(ips)
    engine = Engine(EngineType.RIPE, ips, '380531a9-c3fb-424f-8d1b-23cda9b881fd')
    engine.run()  

if __name__ == "__main__":
    main()