import plotly.express as px  # type: ignore
import plotly.graph_objects as go   # type: ignore
import polars as pl

import HRElectViz.ushelper as ush
from HRElectViz.DistrictsGeoData import DistrictsGeoData
from HRElectViz.GerryMeter import GerryMeter
from HRElectViz.HrElection import (
                      HrElection,
)

gd = DistrictsGeoData('../../map-data-census/tl_2024_us_state.shp',
                      'epsg:4269')

gd.filter_by_state(ush.lower48_abbrs)

# Transform to quasi-mercator so that geometry ccan be simplified;
# simplify with a tolerance of 1 km; the transform abck to lon-lat
gd.xform_geometry('epsg:3857')
gd.simplify(1000.0)
gd.xform_geometry('epsg:4269')


dem_bias_col = 'Partisan\nbias towards\nDemocrats'
hr_elect = HrElection()


dem_bias_df: pl.DataFrame = GerryMeter(hr_elect).get_partisan_bias()

dem_bias_min = dem_bias_df[dem_bias_col].min()
dem_bias_max = dem_bias_df[dem_bias_col].max()
print(f'{dem_bias_min=}', f'{dem_bias_max=}', type(dem_bias_min))
dem_bias_mid = (dem_bias_min + dem_bias_max) / 2.0  # type: ignore
print(f'{dem_bias_mid=}')

dem_bias_df = dem_bias_df.with_columns(
    ((pl.col(dem_bias_col) - dem_bias_min) / (dem_bias_max - dem_bias_min))  # type: ignore
    .alias('color_col'),
    pl.col('State\nAbbr').replace(ush.abbr_to_name).alias('state_name'),
)

redblue_colorscale = [(0.0, 'red'), (0.5, 'white'), (1.0, 'blue')]


##fig = px.choropleth(
##    dem_bias_df,
##    geojson=gd.geojson_data,
##    locations='State\nFIPS',
##    hover_name='state_name',
##    hover_data='tt_text',
##    featureidkey='properties.GEOID',
##    color='color_col',
##    color_continuous_scale=redblue_colorsc,
##    projection='mercator',
##)
other_col_names = [
    'State Vote %\nDemocrat', 'Democrat\ndelegate\ncount',
    'Democrat\ndelegate %', 'State Vote %\nRepublican',
    'Republican\ndelegate\ncount','Republican\ndelegate %',
    'Partisan\nbias towards\nDemocrats', 'State\nAbbr'
]
other_col_stack = dem_bias_df.select(other_col_names).to_numpy().tolist()
fig = go.Figure(go.Choropleth(
    geojson=gd.geojson_data,
    featureidkey='properties.GEOID',
    locations=dem_bias_df['State\nFIPS'],
    z=dem_bias_df['color_col'],
    colorscale=redblue_colorscale,
    colorbar=dict(x=-0.15, y=0.6, len=0.4),
    customdata=other_col_stack,
    hovertemplate=('<b>State: %{customdata[7]}</b><br>' +
        'Democrats: (%{customdata[0]:.2f} %) agg. vote; ' +
        '%{customdata[1]} delegates (%{customdata[2]:.2f} %)<br>' +
        'Republicans: (%{customdata[3]:.2f} %) agg. vote; ' +
        '%{customdata[4]} delegates (%{customdata[5]:.2f} %)<br>' +
        'Partisan skew: %{customdata[6]:.2f} %'
    ),
))
fig.update_geos(fitbounds='locations', visible=False)
fig.update_layout(
    autosize=False,
    width=800,
    height=600,
    title=dict(
        text='U.S. House of Representatives <br>'
        + '2024 Election partisan skew: <br>'
        + '% Democrat representatives v. <br>'
        + '- % Democrat aggregate vote',
        x=0.4,
        y=0.9,
        xanchor='center',
        yanchor='top',
    ),
)

fig.write_html('dem_partisan_skew.html')
