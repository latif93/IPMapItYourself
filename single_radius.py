import os 
import json 
import random 
import requests 
import osmnx as ox
from shapely.geometry import Point
import pytricia 
import pycountry
from constants import AS_TYPE, CITY_TYPE
from probe_selection_configuration import ProbeSelectionConfig
from ASDescriptor import ASDescriptor
from ProbeDescriptor import ProbeDescriptor
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

STATIC_PATH = 'static'
ARTIFACTS_PATH = 'artifacts'

class SingleRadius():
    def __init__(self, pdb_c, ra_c):
        self.pdb_c = pdb_c # PeeringDB Client 
        self.ra_c  = ra_c  # RIPE Atlas Client 

        self.as_neighbour = {}
        self.as_neighbour_fn = f'{ARTIFACTS_PATH}/as_neighbours.json'
        self.addr_to_city_list_fn = f'{ARTIFACTS_PATH}/addr_to_city_list.json'
        self.addr_to_city_list = {} 
        self.measurement_info = []
        self.as_set_sizes = []
        self.one_hop_set_sizes = []
        self.neighbor_set_sizes = []
        self.ixp_set_sizes = []
        self.peeringfac_set_sizes =[]
        self.city_set_sizes = []
        self.target_city_set_sizes = []
        self.ixp_city_set_sizes = []
        self.peeringfac_city_set_sizes = []
        self.pyt = pytricia.PyTricia()
        self.locator = Nominatim(user_agent="kedar")

        
        self.remote = 'https://stat.ripe.net/data/asn-neighbours/data.json'

        self._setup()

    def check_for_completion(self):
        try:
            for t_addr, mid in self.measurement_info:
                if not self.ra_c.is_measurement_complete(mid):
                    return False
            return True
        except Exception as e:
            print("Got error while checking measurement completion, will try later :", e)
            return False

    def _setup(self):
        if os.path.exists(self.as_neighbour_fn):
            with open(self.as_neighbour_fn, 'r') as f:
                self.as_neighbour = json.load(f)

        # Load IP prefix to ASN file 
        with open(f'{STATIC_PATH}/riswhoisdump.IPv4', 'r') as f:
            for line in f:
                if not line.strip() or line.startswith('%'):
                    continue 

                asn, prefix, _ = line.strip().split()
                self.pyt.insert(prefix, asn)
    
    def get_as_neighbours(self, asn):
        # fetch from remote if data does not exist in local cache 
        if asn not in self.as_neighbour: 
            print(f'No cache for ASN {asn}. Fetching from remote...')
            self.as_neighbour[asn] = self.fetch_as_neighbours(asn)
        
        return self.as_neighbour[asn]

    def fetch_as_neighbours(self, asn): 
        try:
            response = requests.get(f"{self.remote}?resource={asn}")
            all_neighbours = sorted(response.json()['data']['neighbours'], key=lambda x: x['power'])

            # select only ases that are one hop away 
            return list([x['asn'] for x in filter(lambda x: x['power'] == 1, all_neighbours)])
        except Exception as e:
            print("Got exception while getting neighbor AS : ", e)
            return []

    def get_addr_asn(self, addr):
        try:
            return self.pyt.get(addr)
        except Exception as e:
            print(e)
            print(addr)
            raise e
    def initial_probe_selection(self, addr, sample_config=ProbeSelectionConfig()):
        """Section 3.1 of paper https://www.caida.org/catalog/papers/2020_ripe_ipmap_active_geolocation/ripe_ipmap_active_geolocation.pdf"""

        A = list()
        C_str    = list() 
        C_coords = list() 
        C_types = list()
        # Step (1): Add AS(t) to A 
        try:
            a_asn = self.get_addr_asn(addr)
            A.append(ASDescriptor(a_asn, AS_TYPE.TARGET))
        except KeyError:
            print(f'Address {addr} can\'t be mapped to ASN')
            return []
        
        # Step (2): Add to C the cities where AS(t) has a probe 
        city_coords = self.ra_c.get_coords_by_asn(int(a_asn))
        C_coords += city_coords 

        # Step (3): Add to A the ASes neighbours (BGP distance of 1) of AS(t)
        neighbours = self.get_as_neighbours(a_asn)
        for neighbor in neighbours:
            A.append(ASDescriptor(a_asn, AS_TYPE.NEIGHBOR))

        # Get network object for target asn (asn is stored as int in pdb client)
        network = self.pdb_c.get_network(int(a_asn))
        
        if network is None: 
            print(f'No network object for address {addr}')
            return []

        # Step (4): Add to C the cities with IXPs where AS(t) is present
        for loc in network.ixp_cities:
            for c in loc.city:
                if c not in C_str:
                    C_str.append(loc)
                    C_types.append(CITY_TYPE.IXP)
        # for city in network.ixp_cities:
        #     if city not in C:
        #         C.append(city)

        # Step (5): Add to A the ASes present at the IXPs identified in step (4) 
        for ixp_as in network.ixp_ases:
            if ixp_as not in A:
                A.append(ASDescriptor(ixp_as, AS_TYPE.IXP))
        
        # Step (6): Add to C all the cities corresponding to the facilites where AS(t) is present
        for loc in network.fac_cities:
            for c in loc.city:
                if c not in C_str:
                    C_str.append(loc)
                    C_types.append(CITY_TYPE.PEERINGFAC)

        # for city in network.fac_cities:
        #     if city not in C:
        #         C.append(city)

        # Step (7): Add to A the ASes peering at facilities identified in step (6)  
        for fac_as in network.fac_ases:
            if fac_as not in A:
                A.append(ASDescriptor(fac_as, AS_TYPE.PEERINGFAC))      

        # Select probes based on the last paragraph in Section 3.1 
        if A or C_str or C_coords:
            self.addr_to_city_list[addr] = {
                'city_str': C_str, 
                'city_coords': C_coords 
            }
            probe_ids = self.select_probes(addr, A, C_str, C_coords, C_types, sample_config)
        else:
            print(f'A & C both empty for address {addr}')
            probe_ids = []

        return probe_ids 

    def select_probes(self, addr, A, C_str, C_coords, C_types, sample_config):
        probe_ids = set()
        try: 
            # Step 1: Select up to 100 random probes from AS(t)
            probes = self.ra_c.get_probes_in_asn(A[0].AS)
            probes = random.sample(probes, min([len(probes), int(100 * sample_config.as_proportion)]))
            probe_ids.update(probes)
            self.as_set_sizes.append(len(probe_ids))
            as_set_size = len(probe_ids)
            remaining_space = 500 - len(probe_ids)

            # Step 2: Select up to 10 random probes from each AS in A
            one_hop_neighbors = dict()
            for asn_desc in A[1:]:  # Skip ASDescriptor that target addr is in
                asn = asn_desc.AS

                if len(one_hop_neighbors.keys()) < remaining_space:
                    asn_p = self.ra_c.get_probes_in_asn(asn)
                    rand_10_probes = random.sample(asn_p, min([len(asn_p), 10]))
                    for probe in rand_10_probes:
                        if probe not in one_hop_neighbors:
                            one_hop_neighbors[probe] = (probe, asn_desc.type)
                else:
                    break 
            temp = list(one_hop_neighbors.values())
            one_hop_neighbors = temp
            one_hop_neighbors = random.sample(one_hop_neighbors, min([len(one_hop_neighbors), int(remaining_space*sample_config.one_hop_proportion)]))

            neighbor_total = 0
            ixp_total = 0
            pfac_total = 0
            for probe, as_type in one_hop_neighbors:
                if probe not in probe_ids:
                    probe_ids.add(probe)
                    if as_type == AS_TYPE.NEIGHBOR:
                        neighbor_total += 1
                    if as_type == AS_TYPE.IXP:
                        ixp_total += 1
                    if as_type == AS_TYPE.PEERINGFAC:
                        pfac_total += 1
            
            self.neighbor_set_sizes.append(neighbor_total)
            self.ixp_set_sizes.append(ixp_total)
            self.peeringfac_set_sizes.append(pfac_total)

            one_hop_total = neighbor_total + ixp_total + pfac_total
            self.one_hop_set_sizes.append(one_hop_total)

            # Step 3: Select up to 50 probes for each city in C
            city_probes = set()
            cache = {}
            remaining_space = 500 - len(probe_ids)
            city_counter = 0 # count how many cities we've processed so far
            target_city_total = 0
            ixp_city_total = 0
            pfac_city_total = 0
            all_probes = self.ra_c.COMPLETE_PROBE_INFO
            if len(probe_ids) < 500:
                for city_coords in C_coords:
                    probes_in_curr_city = []
                    city_coords = reversed([str(coord) for coord in city_coords])
                    location = self.locator.reverse(", ".join(city_coords), language='en')
                    if location and location.address:
                        data = location.raw['address']
                        if 'city' in data and 'country' in data:
                            try:
                                country = pycountry.countries.search_fuzzy(data['country'])[0].alpha_2
                                key = ', '.join([data['city'], country])
                                print(key)
                                if key in cache:
                                    gdf = cache[key]
                                else:
                                    gdf = ox.geocode_to_gdf(', '.join([data['city'], data['country']]))
                                    cache[key] = gdf
                                
                                geom = gdf.loc[0, 'geometry']
                            except Exception as e:
                                print(e)
                                continue
                            for probe in all_probes:
                                if probe is None:
                                    continue
                                geometry = probe['geometry']
                                if geometry is None:
                                    continue
                                coords = geometry['coordinates']
                                if probe["id"] not in probe_ids and probe["id"] not in city_probes and (target_city_total + len(probe_ids)) < remaining_space:
                                    if geom.intersects(Point(coords)):
                                        probe_id = probe['id']
                                        probes_in_curr_city.append(probe_id)
                                        target_city_total += 1
                    probes_in_curr_city = random.sample(probes_in_curr_city, min(len(probes_in_curr_city), 50))
                    city_probes.update(probes_in_curr_city)
                    if len(city_probes) > remaining_space:
                        break

                            
                for city in set(C_str):
                    probes_in_curr_city = []
                    try:
                        key = ', '.join(city)
                        if key in cache:
                            gdf = cache[key]
                        else:
                            gdf = ox.geocode_to_gdf(', '.join(city))
                            cache[key] = gdf
                        print(key)
                        geom = gdf.loc[0, 'geometry']
                    except:
                        continue
                    
                    for probe in all_probes:
                        if ixp_city_total + pfac_city_total > remaining_space:
                            break
                        if probe is None:
                            continue
                        geometry = probe['geometry']
                        if geometry is None:
                            continue

                        coords = geometry['coordinates']
                        if probe["id"] not in probe_ids and probe["id"] not in city_probes:
                            if geom.intersects(Point(coords)):
                                probe_id = probe['id']
                                probes_in_curr_city.append(probe_id)
                                if C_types[city_counter] == CITY_TYPE.IXP:
                                    ixp_city_total += 1
                                elif C_types[city_counter] == CITY_TYPE.PEERINGFAC:
                                    pfac_city_total += 1
                        
                    city_counter += 1

                            


                    probes_in_curr_city = random.sample(probes_in_curr_city, min(len(probes_in_curr_city), 50))
                    city_probes.update(probes_in_curr_city)
                    if len(city_probes) > remaining_space:
                        break
                




            remaining_space = 500 - len(probe_ids)
            city_probes = random.sample(city_probes, min(len(city_probes), remaining_space))
            self.city_set_sizes.append(len(city_probes))
            self.ixp_city_set_sizes.append(ixp_city_total)
            self.target_city_set_sizes.append(target_city_total)
            self.peeringfac_city_set_sizes.append(pfac_city_total)
            probe_ids.update(city_probes)
            probe_ids = set(probe_ids)

            print("Got set sizes -- Target : {} |  One Hop Total : {} | Neighbor : {} | IXP : {} | PeeringFac : {} | City Target: {} | City IXP : {} | City PFac : {} | Total {}".format(
                as_set_size, one_hop_total, neighbor_total, ixp_total, pfac_total, 0, ixp_city_total, pfac_city_total, len(probe_ids)
            ))

        except KeyError:
            print("lol")

        if not probe_ids:
            print(f'No RIPE probe in AS for address {addr}')

        return [str(p_id) for p_id in probe_ids]

    def select_random_probes(self):
        probes = self.ra_c.get_all_probes() 
        return [str(addr) for addr in random.sample(probes, 300)]

    def measure_addr(self, addr):
        probes = self.initial_probe_selection(addr)

        if not probes: # probes is empty
            probes = self.select_random_probes() 
        t_addr, m_id = self.ra_c.create_measurement(addr, probes)
        if t_addr is not None and m_id is not None:
            self.measurement_info.append((t_addr, m_id))

    def terminate(self):
        with open(self.as_neighbour_fn, 'w') as f:
            json.dump(self.as_neighbour, f)
        
        with open(self.addr_to_city_list_fn, 'w') as f:
            json.dump(self.addr_to_city_list, f)
        
        self.ra_c.terminate()