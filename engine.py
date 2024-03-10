from datetime import datetime, timedelta
import json
import time
import pandas as pd
from constants import DF_COLS, EngineType
from single_radius import SingleRadius
from geolocator import Geolocator
from ripe_atlas_client import RIPEAtlasClient
from ripe.atlas.cousteau import AtlasResultsRequest
from pdbutils import PeeringDB
from tqdm import tqdm

class Engine:
    def __init__(self, engine_type, ips, api_key=None, validation=False):
        self.engine_type = engine_type
        self.api_key = api_key
        self.ips = ips
        self.validation = validation
        if self.engine_type == EngineType.RIPE:
            assert self.api_key is not None, "API key is required for RIPE engine type"
            self.client = RIPEAtlasClient(api_key)
        else:
            self.client = None  # or other client initialization for different engine types
        self.single_radius = SingleRadius(PeeringDB(), self.client)
        self.geolocator = Geolocator(self.client)
        self.results = []

    def run(self):
        start_time = datetime.now()
        max_duration = timedelta(minutes=30)  # Maximum duration to wait for all measurements

        print(f"Processing started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        for ip in tqdm(self.ips, desc="Measuring IP addresses"):
            self.single_radius.measure_addr(ip)
            # Insert a minimal delay if necessary to avoid rate limiting issues
            time.sleep(0.1)  # Adjust as necessary based on the API's rate limit

        while not self.single_radius.check_for_completion():
            current_time = datetime.now()
            if current_time - start_time > max_duration:
                print("Maximum measurement duration reached, proceeding with available results.")
                break
            print("Checking if all measurements are complete")
            time.sleep(60)  # Check every minute

        for t_addr, m_id in tqdm(self.single_radius.measurement_info, desc="Fetching measurement results"):
            is_success, results = AtlasResultsRequest(msm_id=m_id).create()
            if is_success:
                result = self.geolocator.get_loc(t_addr, results)
                if result:
                    self.results.append((t_addr,) + result)
                else:
                    print(f"Skipping {t_addr} due to failed location retrieval.")
            else:
                print(f"Measurement for {t_addr} did not succeed.")

        if self.results:
            df = pd.DataFrame(data=self.results, columns=DF_COLS)
            df.to_csv('results.csv')
            print('Wrote Results To File')
        else:
            print("No results were obtained to write to file.")

        end_time = datetime.now()
        print(f"Processing finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        duration = end_time - start_time
        print(f"Total processing time: {duration}")

def main():
    ips = []
    with open("final_processed.json", "r") as f:
        for line in f.readlines():
            data = json.loads(line)
            ipv4 = data['ip_addr']
            if ipv4 is not None:
                ips.append(ipv4)
    engine = Engine(EngineType.RIPE, ips, 'b6ee5451-b96f-4434-b826-a343a611e9ee', validation=False)
    engine.run()

if __name__ == "__main__":
    main()
