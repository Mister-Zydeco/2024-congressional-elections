import polars as pl

from hrelectviz.districtsgeodata import DistrictsGeoData
from hrelectviz.hrelection import HrElection, std_polars_config

if __name__ == '__main__':
    hr_elect_dfs: HrElection = HrElection()
    district_winners: pl.DataFrame = hr_elect_dfs.get_district_winners()
    gd = DistrictsGeoData(
        'map-data-ntad/Congressional_Districts.shp', 'epsg:3857'
    )
    geo_df: pl.DataFrame = pl.from_dict(gd.get_props(['LASTNAME']))
    winners_plus_geo = district_winners.join(geo_df, on='GEOID')
    with std_polars_config():
        print(winners_plus_geo)
