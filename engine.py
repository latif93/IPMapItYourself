from datetime import datetime
import json
import time
import pandas as pd
from tqdm import tqdm
from constants import DF_COLS, EngineType
from single_radius import SingleRadius
from geolocator import Geolocator
from ripe_atlas_client import RIPEAtlasClient
from ripe.atlas.cousteau import AtlasResultsRequest
from pdbutils import PeeringDB
# Assuming all necessary imports are done

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
            self.client = None  # Placeholder for other client initializations
        self.single_radius = SingleRadius(PeeringDB(), self.client)
        self.geolocator = Geolocator(self.client)
        self.results = []

    def run(self):
        start_time = datetime.now()
        print(f"Processing started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        for ip in tqdm(self.ips, desc="Measuring IP addresses"):
            self.single_radius.measure_addr(ip)
            time.sleep(0.1)  # Adjust based on API limits

        # Wait for all measurements to complete without a maximum duration
        while not self.single_radius.check_for_completion():
            print("Checking if all measurements are complete")
            time.sleep(60)  # Adjust the sleep time as needed

        for t_addr, m_id in tqdm(self.single_radius.measurement_info, desc="Fetching measurement results"):
            is_success, results = AtlasResultsRequest(msm_id=m_id).create()
            if is_success:
                result = self.geolocator.get_loc(t_addr, results)
                if result:  # Make sure result is not None
                    self.results.append((t_addr,) + tuple(result))
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
            ipv4 = data.get('ip_addr')
            if ipv4:
                ips.append(ipv4)
    # Replace 'your_api_key_here' with your actual RIPE Atlas API key.
    engine = Engine(EngineType.RIPE, ips, 'b6ee5451-b96f-4434-b826-a343a611e9ee', validation=False)
    engine.run()

if __name__ == "__main__":
    main()
