import json 
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from math import radians, degrees, sin, cos, asin, acos, sqrt

def great_circle(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    return 6371 * (
        acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon1 - lon2))
    )

class Evaluator():
    def __init__(self, results=None):
        self.results = results
        with open('static/Combined.json', 'r') as f:
            self.ipmap = json.load(f)

        with open('maxmind.json', 'r') as f:
            self.maxmind = json.load(f)

        with open('ipinfo.json', 'r') as f:
            self.ipinfo = json.load(f)

        self.err_dist = list() 

    def evaluate(self):
        if self.results is None:
            self.results = pd.read_csv("artifacts/results.csv")
        for index, row in self.results.iterrows():
            addr, city, c_code, c_ascii, t_lon, t_lat, m_id = row
            if city == 'NAN':
                continue 
        
            t_lon, t_lat = float(t_lon), float(t_lat)
            c_lon, c_lat = float(self.correct[addr]['lon']), float(self.ipmap[addr]['lat'])

            e_d = great_circle(t_lon, t_lat, c_lon, c_lat)

            self.err_dist.append(e_d)


        self.self.ipmap_err_dist = sorted(self.self.ipmap_err_dist) 
        self.mmind_err_dist = sorted(self.mmind_err_dist)
        self.iinfo_err_dist = sorted(self.iinfo_err_dist)
        self.self.ipmap_err_dist = list(map(lambda x: x if x > 1 else 1, self.self.ipmap_err_dist))
        self.mmind_err_dist = list(map(lambda x: x if x > 1 else 1, self.mmind_err_dist))
        self.iinfo_err_dist = list(map(lambda x: x if x > 1 else 1, self.iinfo_err_dist))
    
    def plot(self, fig_name="error.jpg"):
        df_t = pd.DataFrame({'single-radius': self.self.ipmap_err_dist, 'MaxMind': self.self.mmind_err_dist, 'IPinfo': self.iinfo_err_dist})
        df_m = df_t.melt(var_name='dataset')
        fig, ax = plt.subplots()
        fig.set_size_inches(10, 5)
        sns.ecdfplot(
            ax=ax,
            data=df_t, 
            x='single-radius',
            label='single-radius',
            log_scale=True,
            color='blue'
        )
        sns.ecdfplot(
            ax=ax,
            data=df_t, 
            x='IPinfo',
            label='IPinfo',
            log_scale=True,
            ls=':',
            color='orange'
        )
        sns.ecdfplot(
            ax=ax,
            data=df_t, 
            x='MaxMind',
            label='MaxMind',
            log_scale=True,
            ls=':',
            color='green'
        )


        ax.legend()
        ax.set_xlim(0.75, 15000)
        ax.set_yticks([0.00,0.25,0.5,0.75, 1.00])
        ax.set_xticks([1, 10, 40, 100, 1000, 10000])
        ax.set_ylim(0, 1.05)
        ax.set_xlabel('Error Distance from Ground Truth Location (km)', size='x-large')
        ax.set_ylabel('CDF', size='x-large')

        # ax.set_xticks(range(1,32))
        plt.xticks(fontsize='large')
        plt.yticks(fontsize='large')
        plt.axvline(x=40, color='magenta', linestyle="dashed")
        plt.legend(loc='lower right', fontsize='large')
        plt.grid()
        # plt.show()
        fig.savefig(fig_name, dpi=300, bbox_inches='tight')

if __name__ == "__main__":
    df = pd.read_csv("artifacts/results.csv")
    evaluator = Evaluator(df)
    evaluator.evaluate()
    evaluator.plot()