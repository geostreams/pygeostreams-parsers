import geopandas as gpd
import pandas as pd
import shapely.wkt
from geopandas.tools import sjoin


class HucFinder:
    def __init__(self, huc_data_file):
        self.hucData = gpd.GeoDataFrame.from_file(huc_data_file)

    def getHuc(self, lat, lon):
        # create a geodataframe with lat/lon
        wkt = 'POINT(' + str(lon) + ' ' + str(lat) + ')'
        geometry = [shapely.wkt.loads(wkt)]
        crs = {'init': 'epsg:4269'}
        point = gpd.GeoDataFrame(pd.DataFrame({'id': [0]}), crs=crs, geometry=geometry)
        try:
            # find a huc polygon contains the point
            huc = sjoin(point, self.hucData, how='inner', op='intersects')

            # if there is no huc reutrn empty dictionary
            if len(huc.index) == 0:
                return {}

            # if there is a huc, create a dictionary
            result = {
                'huc_name': huc['HUC_NAME'][0],
                'huc2': {'code': huc['REG'][0]},
                'huc4': {'code': huc['SUB'][0]},
                'huc6': {'code': huc['ACC'][0]},
                'huc8': {'code': huc['CAT'][0]}
            }
            return result
        except ValueError:
            # if there is no huc return empty dictionary
            return {}
