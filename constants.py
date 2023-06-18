from enum import Enum

STOPPED_STATUSES = set(['Stopped', 'Forced to stop', 'No suitable probes', 'Failed or Archived'])
DF_COLS = ['Target Addr', 'City', 'CC', 'Country', 'Probe Longitude', 'Probe Latitude', 'Probe ID']
EngineType = Enum('EngineType', ['RIPE', 'LOCAL'])
NUM_VANTAGE_POINTS = 500
AS_TYPE = Enum('ASType', ['TARGET', 'NEIGHBOR', 'IXP', 'PEERINGFAC'])
PROBE_TYPE = Enum('ProbeType', ['TARGET', 'NEIGHBOR', 'IXP', 'PEERINGFAC'])
CITY_TYPE = Enum('CityType', ['Target', 'IXP', 'PEERINGFAC'])