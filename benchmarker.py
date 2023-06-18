from single_radius import SingleRadius
from pdbutils import PeeringDB
from probe_selection_configuration import ProbeSelectionConfig
from ripe_atlas_client import RIPEAtlasClient
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
class Benchmarker():
    def __init__(self, ripe_atlas_client=None):
        """
        See ripe_atlas_client.py
        """
        self.ripe_atlas_client = ripe_atlas_client
        self.single_radius = SingleRadius(PeeringDB(), ripe_atlas_client)
        self.results = pd.read_csv("artifacts/results.csv")

        self.ONE_HOP_CSV_NAME = 'artifacts/onehop.csv'
        self.NEIGHBOR__CSV_NAME = 'artifacts/neighbor.csv'
        self.IXP_CSV_NAME  = 'artifacts/ixp.csv'
        self.PFAC_CSV_NAME  = 'artifacts/pfac.csv'

        self.TARGET_CITY_CSV_NAME = 'artifacts/targetcity.csv'
        self.IXP_CITY_CSV_NAME = 'artifacts/ixpcity.csv'
        self.PFAC_CITY_CSV_NAME = 'artifacts/pfaccity.csv'
        self.CITY_CSV_NAME = 'artifacts/city.csv'

    def test_sampling_two_parts(self, sample_config=ProbeSelectionConfig()):
        self.do_first_part(sampleconfig=sample_config)
        self.do_second()

    def do_first_part(self, sampleconfig=ProbeSelectionConfig()):

        correct = 0
        total = 0
        for index, row in self.results.iterrows():
            correct_probe = row['Probe ID']
            if correct_probe == 'NAN':
                continue
            selected_probes = self.single_radius.initial_probe_selection(row['Target Addr'], sample_config)
            if correct_probe in selected_probes:
                correct += 1
            total += 1
        
        as_df = pd.DataFrame(data=self.single_radius.as_set_sizes)



        print(self.single_radius.ixp_city_set_sizes)
        one_hop_set_sizes_df = pd.DataFrame({'value': self.single_radius.one_hop_set_sizes, 'type': 'One Hop'})
        neighbor_set_sizes_df = pd.DataFrame({'value': self.single_radius.neighbor_set_sizes, 'type': 'Neighbor'})
        ixp_set_sizes_df = pd.DataFrame({'value': self.single_radius.ixp_set_sizes, 'type': 'IXP'})
        peering_fac_set_sizes_df = pd.DataFrame({'value': self.single_radius.peeringfac_set_sizes, 'type': 'Peering Fac'})
        one_hop_set_sizes_df.to_csv(self.ONE_HOP_CSV_NAME)
        neighbor_set_sizes_df.to_csv(self.NEIGHBOR__CSV_NAME)
        ixp_set_sizes_df.to_csv(self.IXP_CSV_NAME)
        peering_fac_set_sizes_df.to_csv(self.PFAC_CITY_CSV_NAME)




         #Combine all dataframes for city data
        target_city_set_sizes_df = pd.DataFrame({'value': self.single_radius.target_city_set_sizes, 'type': 'Target'})
        ixp_city_set_sizes_df = pd.DataFrame({'value': self.single_radius.ixp_city_set_sizes, 'type': 'IXP'})
        pfac_city_set_sizes_df = pd.DataFrame({'value': self.single_radius.peeringfac_city_set_sizes, 'type': 'Peering Facility'})
        total_city_set_Sizes_df = pd.DataFrame({'value': self.single_radius.city_set_sizes, 'type': 'Total'})

        target_city_set_sizes_df.to_csv(self.TARGET_CITY_CSV_NAME)
        ixp_city_set_sizes_df.to_csv(self.IXP_CITY_CSV_NAME)
        pfac_city_set_sizes_df.to_csv(self.PFAC_CSV_NAME)
        total_city_set_Sizes_df.to_csv(self.CITY_CSV_NAME)


        print("TARGET CITY SET SIZES:")
        print(self.single_radius.target_city_set_sizes)
        print("IXP CITY SET SIZES:")
        print(self.single_radius.ixp_city_set_sizes)
        print("PFAC CITY SET SIZES:")
        print(self.single_radius.peeringfac_city_set_sizes)
        print("TOTAL CITY SET SIZES:")
        print(self.single_radius.city_set_sizes)
    
    def do_second(self):

        one_hop_set_sizes_df = pd.read_csv(self.ONE_HOP_CSV_NAME)
        neighbor_set_sizes_df = pd.read_csv(self.NEIGHBOR__CSV_NAME)
        ixp_set_sizes_df = pd.read_csv(self.IXP_CSV_NAME)
        pfac_set_sizes_df = pd.read_csv(self.PFAC_CSV_NAME)

        target_city_set_sizes_df = pd.read_csv(self.TARGET_CITY_CSV_NAME)
        ixp_city_set_sizes_df = pd.read_csv(self.IXP_CITY_CSV_NAME)
        pfac_city_set_sizes_df = pd.read_csv(self.PFAC_CITY_CSV_NAME)
        total_city_set_Sizes_df = pd.read_csv(self.CITY_CSV_NAME)

        # Combine all dataframes
        combined_df = pd.concat([one_hop_set_sizes_df, neighbor_set_sizes_df, ixp_set_sizes_df, pfac_set_sizes_df])
        # Ensure the value column is numeric and NaN values are handled
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        combined_df = combined_df.dropna(subset=['value'])
        # Ensure the value column is of int type
        combined_df['value'] = combined_df['value'].astype(int)
        # Sort values for accurate ECDF
        combined_df = combined_df.sort_values('value')
        # Calculate ECDF
        combined_df['ECDF'] = combined_df.groupby('type').value.transform(lambda x: np.arange(1, len(x) + 1) / len(x))
        combined_df.to_csv('artifacts/combined_data_step_2.csv')

        combined_df_city = pd.concat([target_city_set_sizes_df, ixp_city_set_sizes_df, pfac_city_set_sizes_df, total_city_set_Sizes_df])
        combined_df_city['value'] = pd.to_numeric(combined_df_city['value'], errors='coerce')
        combined_df_city = combined_df_city.dropna(subset=['value'])
        combined_df_city['value'] = combined_df_city['value'].astype(int)
        combined_df_city = combined_df_city.sort_values('value')
        combined_df_city['ECDF'] = combined_df_city.groupby('type').value.transform(lambda x: np.arange(1, len(x) + 1) / len(x))
        combined_df_city.to_csv('artifacts/combined_data_step_3.csv')





    
    def test_sampling(self, sample_config=ProbeSelectionConfig()):
        correct = 0
        total = 0
        for index, row in self.results.iterrows():
            correct_probe = row['Probe ID']
            if correct_probe == 'NAN':
                continue
            selected_probes = self.single_radius.initial_probe_selection(row['Target Addr'], sample_config)
            if correct_probe in selected_probes:
                correct += 1
            total += 1
        
        as_df = pd.DataFrame(data=self.single_radius.as_set_sizes)



        print(self.single_radius.ixp_city_set_sizes)
        one_hop_set_sizes_df = pd.DataFrame({'value': self.single_radius.one_hop_set_sizes, 'type': 'Total'})
        neighbor_set_sizes_df = pd.DataFrame({'value': self.single_radius.neighbor_set_sizes, 'type': 'Neighbor'})
        ixp_set_sizes_df = pd.DataFrame({'value': self.single_radius.ixp_set_sizes, 'type': 'IXP'})
        peering_fac_set_sizes_df = pd.DataFrame({'value': self.single_radius.peeringfac_set_sizes, 'type': 'Peering Fac'})
        one_hop_set_sizes_df.to_csv(self.ONE_HOP_CSV_NAME)
        neighbor_set_sizes_df.to_csv(self.NEIGHBOR__CSV_NAME)
        ixp_set_sizes_df.to_csv(self.IXP_CSV_NAME)
        peering_fac_set_sizes_df.to_csv(self.PFAC_CSV_NAME)

        # Combine all dataframes
        combined_df = pd.concat([one_hop_set_sizes_df, neighbor_set_sizes_df, ixp_set_sizes_df, peering_fac_set_sizes_df])

        # Ensure the value column is numeric and NaN values are handled
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        combined_df = combined_df.dropna(subset=['value'])

        # Ensure the value column is of int type
        combined_df['value'] = combined_df['value'].astype(int)

        # Sort values for accurate ECDF
        combined_df = combined_df.sort_values('value')

        # Calculate ECDF
        combined_df['ECDF'] = combined_df.groupby('type').value.transform(lambda x: np.arange(1, len(x) + 1) / len(x))
        combined_df.to_csv('artifacts/combined_data_step_2.csv')

        #Combine all dataframes for city data
        target_city_set_sizes_df = pd.DataFrame({'value': self.single_radius.target_city_set_sizes, 'type': 'Target'})
        ixp_city_set_sizes_df = pd.DataFrame({'value': self.single_radius.ixp_city_set_sizes, 'type': 'IXP'})
        pfac_city_set_sizes_df = pd.DataFrame({'value': self.single_radius.peeringfac_city_set_sizes, 'type': 'PFAC'})
        total_city_set_Sizes_df = pd.DataFrame({'value': self.single_radius.city_set_sizes, 'type': 'Total'})
        combined_df_city = pd.concat([target_city_set_sizes_df, ixp_city_set_sizes_df, pfac_city_set_sizes_df])


        print("TARGET CITY SET SIZES:")
        print(self.single_radius.target_city_set_sizes)
        print("IXP CITY SET SIZES:")
        print(self.single_radius.ixp_city_set_sizes)
        print("PFAC CITY SET SIZES:")
        print(self.single_radius.peeringfac_city_set_sizes)
        print("TOTAL CITY SET SIZES:")
        print(self.single_radius.city_set_sizes)

        print(combined_df_city)
        combined_df_city['value'] = combined_df_city['value'].astype(int)
        combined_df_city = combined_df_city.sort_values('value')
        combined_df_city['ECDF'] = combined_df_city.groupby('type').value.transform(lambda x: np.arange(1, len(x) + 1) / len(x))
        combined_df_city.to_csv('artifacts/combined_data_step_3.csv')

        city_df = pd.DataFrame(data=self.single_radius.city_set_sizes)
        city_df.to_csv('artifacts/cityresults.csv')
        total_data_set = []
        for (assize, ohsize, citysize) in zip(self.single_radius.as_set_sizes, self.single_radius.one_hop_set_sizes, self.single_radius.city_set_sizes):
            total_data_set.append(assize + ohsize + citysize)
        total_df = pd.DataFrame(data=total_data_set)
        total_df.to_csv('artifacts/totalresults.csv')

        return correct/total

    def full_test(self, sample_config=ProbeSelectionConfig()):
        results = []
        for i in range(100):
            results.append(self.test_sampling(sample_config))
        return results
    
    def plot_results(self):
        combined_df_as= pd.read_csv('artifacts/combined_data_step_2.csv')
        combined_df_city = pd.read_csv('artifacts/combined_data_step_3.csv')

        sns.lineplot(data=combined_df_as, x="value", y="ECDF", hue="type")
        plt.title("ECDF of Set Sizes")
        plt.xlabel("Set Sizes")
        plt.ylabel("ECDF")
        plt.grid(True)
        plt.savefig("artifacts/Step2SetSizes.png")

        sns.lineplot(data=combined_df_city, x="value", y="ECDF", hue="type")
        plt.title("ECDF of Set Sizes")
        plt.xlabel("Set Sizes")
        plt.ylabel("ECDF")
        plt.grid(True)
        plt.savefig("artifacts/Step3SetSizes.png")

        as_df = pd.read_csv('artifacts/ASresults.csv')
        sns.ecdfplot(data=as_df)
        plt.title("AS Probe Set Size Distribution")
        plt.savefig("artifacts/ASSetSizes.png")


if __name__ == "__main__":
    ripe_atlas_client = RIPEAtlasClient(api_key='380531a9-c3fb-424f-8d1b-23cda9b881fd')
    benchmarker = Benchmarker(ripe_atlas_client=ripe_atlas_client)
    sample_config = ProbeSelectionConfig(as_proportion=1, one_hop_proportion=1)
    results = benchmarker.test_sampling_two_parts(sample_config=sample_config)
    #benchmarker.plot_results()
    print(results)
