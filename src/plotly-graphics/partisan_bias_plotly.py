import polars as pl
from HRElectViz.HrElection import HrElection
from HRElectViz.HrElection import std_polars_config, lower48_selector
from HRElectViz.GerryMeter import GerryMeter
from HRElectViz.DistrictsGeoData import DistrictsGeoData
import plotly.express as px  # type: ignore
import os

os.environ["DC_STATEHOOD"] = "1"
import us  # type: ignore

print(us.states.mapping("fips", "abbr"))

gd = DistrictsGeoData("../../map-data-ntad/Congressional_Districts.shp", "epsg:3857")
gd.simplify(1000.0)
gd.xform_geometry("epsg:4326")

print(gd.as_str()[:400])

dem_partisan_bias = "Partisan\nbias towards\nDemocrats"
hr_elect = HrElection()

sds = (
    hr_elect.get_district_winners()
    .select(["State\nCode", "GEOID"])
    .filter(lower48_selector)
)


partisan_bias_pl = (
    GerryMeter(hr_elect)
    .get_partisan_bias()
    .join(sds, on="State\nCode")
    .with_columns(((pl.col(dem_partisan_bias) + 100.0) / 200.0).alias("color_col"))
)

with std_polars_config():
    print(
        partisan_bias_pl.select(["State\nCode", "color_col"])
        .group_by("State\nCode")
        .first()
        .sort("color_col", descending=True)
    )


redblue_colorsc = [[0, "red"], [1, "blue"]]
fig = px.choropleth(
    partisan_bias_pl,
    geojson=gd.geojson_data,
    locations="GEOID",
    featureidkey="properties.GEOID",
    color="color_col",
    color_continuous_scale=redblue_colorsc,
    projection="mercator",
)
fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(
    autosize=False,  # Disable autosizing to manually set dimensions
    width=1200,  # Set the width of the figure in pixels
    height=900,  # Set the height of the figure in pixels
)
fig.write_html("dem_partisan_bias.html")
