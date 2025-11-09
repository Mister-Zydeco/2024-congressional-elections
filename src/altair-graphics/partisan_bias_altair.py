import polars as pl
from HRElectViz.HrElection import HrElection, std_polars_config
from HRElectViz.GerryMeter import GerryMeter
from HRElectViz.DistrictsGeoData import DistrictsGeoData
import altair as alt
import os

os.environ["DC_STATEHOOD"] = "1"
from HRElectViz import ushelper as ush

print(f"fips code for NJ={ush.abbr_to_fips['NJ']}")


dem_partisan_bias = "Partisan\nbias towards\nDemocrats"
new_dem_bias = "Democrat-leaning bias"
hr_elect = HrElection()

sds = hr_elect.get_district_winners().select(["State\nCode", "GEOID"])

lower48_selector = pl.col("State\nCode").is_in(ush.lower48_abbrs)

partisan_bias_pl = (
    GerryMeter(hr_elect)
    .get_partisan_bias()
    .join(sds, on="State\nCode")
    .with_columns(((pl.col(dem_partisan_bias) + 100.0) / 200.0).alias("color_col"))
    .rename({dem_partisan_bias: new_dem_bias})
).select(["GEOID", "color_col", "State\nCode"])

partisan_bias_reduced = (
    partisan_bias_pl.group_by("State\nCode")
    .first()
    .sort(pl.col("color_col"), descending=True)
)

with std_polars_config():
    print(partisan_bias_reduced)


gd = DistrictsGeoData("../../map-data-ntad/Congressional_Districts.shp", "epsg:3857")
gd.simplify(1000.0)
gd.xform_geometry("epsg:4326")
gd.filter_by_state(ush.lower48_abbrs)
# gd.filter_by_state(['NJ'])
geodata = alt.Data(values=gd.geojson_data["features"])


base = (
    alt.Chart(geodata)
    .mark_geoshape(stroke="black", fill=None, strokeWidth=0.25)
    .project("albersUsa")
    .properties(width=1200, height=800)
)


base.save("base.png")
print("Saved base")


color_domain, color_range = [0.0, 1.0], ["red", "blue"]
choropleth = (
    alt.Chart(geodata)
    .mark_geoshape()
    .project(type="albersUsa")
    .transform_lookup(
        lookup="properties.GEOID",
        from_=alt.LookupData(partisan_bias_pl, "GEOID", ["color_col"]),
    )
    .encode(
        color=alt.Color(
            shorthand="color_col:Q", scale=alt.Scale(domainMid=0.5, range="diverging")
        )
    )
    .properties(width=1200, height=800, title="Democrat-leaning bias")
)
chart = base + choropleth
chart.save("chart.html")
