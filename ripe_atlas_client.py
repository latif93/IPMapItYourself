import json 
import time 
import pprint
import datetime 
from ripe.atlas.cousteau import Ping, AtlasSource, AtlasCreateRequest, Measurement
from constants import STOPPED_STATUSES

STATIC_PATH = 'static'

class RIPEAtlasClient():
    def __init__(self, api_key=None):
        
        # Bert's api key: b6ee5451-b96f-4434-b826-a343a611e9ee
        #Ali's api key: 24725bf1-7ddf-4986-a826-0eb4bedafaac

        self.api_key = api_key if api_key is not None else 'b6ee5451-b96f-4434-b826-a343a611e9ee'
        self.log_fname = f"artifacts/measurements.{datetime.datetime.now().strftime('%Y-%m-%d-%H_%M_%S')}.csv"
        self.log_f = None 

        self.ALL_PROBES        = [] # all RIPE probe ids
        self.ASN_TO_RIPE_PROBE = {} # asn      to RIPE probe 
        self.PID_TO_RIPE_PROBE = {} # probe id to RIPE probe 
        self.COMPLETE_PROBE_INFO = {}
        self.live_measurements = set()

        self._setup()
    

    def _setup(self): 
        with open(f'{STATIC_PATH}/RIPE_Probes.json', 'r') as f:
            probes = json.load(f)
            self.COMPLETE_PROBE_INFO = probes

        for probe in probes:
            if probe['status']['name'] != 'Connected':
                continue
                
            asn = probe['asn_v4']
            pid = probe['id']

            self.ALL_PROBES.append(pid)
            
            if asn not in self.ASN_TO_RIPE_PROBE:
                self.ASN_TO_RIPE_PROBE[asn] = [probe]
            else:
                self.ASN_TO_RIPE_PROBE[asn] += [probe]
                
            if pid not in self.PID_TO_RIPE_PROBE:
                self.PID_TO_RIPE_PROBE[pid] = probe
        
        self.log_f = open(f'{self.log_fname}', 'w')
    
    def get_probes_in_asn(self, asn):
        probes = []
        if asn in self.ASN_TO_RIPE_PROBE:
            probes = [probe['id'] for probe in self.ASN_TO_RIPE_PROBE[asn]]
        return probes
    
    def get_coords_by_asn(self, asn, country = None):
        coords = [] 
        if asn in self.ASN_TO_RIPE_PROBE:
            if country:
                for coord in self.ASN_TO_RIPE_PROBE[asn]:
                    if coord['country'] == country:
                      coords += coord['geometry']['coordinates']
            else:
                coords = [coord['geometry']['coordinates'] for coord in self.ASN_TO_RIPE_PROBE[asn]]
        
        return coords 
    
    def is_measurement_complete(self, mid):
        msmt = Measurement(id=mid)
        return msmt.status in STOPPED_STATUSES
    
    def check_and_clean_measurements(self):
        measurements_to_remove = set()
        for mid in self.live_measurements:
            if self.is_measurement_complete(mid):
                measurements_to_remove.add(mid)
        self.live_measurements = self.live_measurements - measurements_to_remove
        

    def create_measurement(self, t_addr, probes, m_type='ping'):
        if not probes:
            # self.log_f.write(f'{t_addr},0\n')
            print(f'No probes for {t_addr}...')
            return 

        while len(self.live_measurements) >= 100:
            print('Cleaning up measurements')
            self.check_and_clean_measurements()
            time.sleep(1)

        print(f'Creating measurement for {t_addr}...')
        if m_type == 'ping':
            ping = Ping(af=4, target=t_addr, description=f"SingleRadius to {t_addr}")
        else:
            raise NotImplementedError
        try:
            source = AtlasSource(
                type='probes', 
                value=','.join(probes),
                requested=len(probes), 
                tags={"include":["system-ipv4-works"]}
                )
        except TypeError:
            print(f'Type error when creating measurement for address {t_addr}')
            return 
        
        a_request = AtlasCreateRequest(
            start_time=datetime.datetime.utcnow(),
            # stop_time=datetime.datetime.utcnow()+datetime.timedelta(seconds=20),
            key=self.api_key,
            measurements=[ping], 
            sources=[source],
            is_oneoff=True
        )

        is_success, response = a_request.create()
    
        if is_success:
            m_id = response['measurements'][0] # measurement id 
            self.log_f.write(f'{t_addr},{m_id}\n')
            self.live_measurements.add(m_id)

            if m_id == 0:
                pprint.pprint(response)

            return t_addr, m_id  
        else:
            try:
                err = response['error']

                if err['code'] == 102: # You are not permitted to run more than 100 concurrent measurements
                    print('Sleeping for 1 mins...')
                    time.sleep(1*60)
                    return None, None
                else:
                    pprint.pprint(err)
                    return None, None
        
            except:
                pprint.pprint(response)

    def get_all_probes(self):
        return self.ALL_PROBES

    def terminate(self):
        self.log_f.close()


if __name__ == '__main__':
    ra_c = RIPEAtlasClient() 
    #print(ra_c.get_coords_by_asn(206238)) 


    
