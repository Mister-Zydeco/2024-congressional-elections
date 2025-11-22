import re
import plotly.express as px  # type: ignore
import plotly.graph_objects as go   # type: ignore
import polars as pl

import HRElectViz.ushelper as ush
from HRElectViz.HrElection import HrElection
from HRElectViz.DistrictsGeoData import DistrictsGeoData
from HRElectViz.GerryMeter import GerryMeter, major_parties

nl = '\n'
color_column_names: dict[str, dict[str, str]] = {
    'partisan_skew':
        dict((party, f'Skew towards{nl}{party}') for party in major_parties),
    'mean_median_difference':
        dict((party,
            f'Mean-median difference{nl}(+ favors {major_parties[1 - ix]}s)')
            for ix, party in enumerate(major_parties)),
    'efficiency_gap':
        dict((party, f'{party}-leaning{nl}efficiency gap')
             for party in major_parties)
}



def get_districts_geodata(path: str, projection: str) -> DistrictsGeoData:
    gd = DistrictsGeoData(path, projection)
    gd.filter_by_state(ush.lower48_abbrs)

    # Transform to quasi-mercator so that geometry ccan be simplified;
    # simplify with a tolerance of 1 km; then transform back to lon-lat
    if projection != 'epsg:3857':
        gd.xform_geometry('epsg:3857')
    gd.simplify(1000.0)
    if projection != 'epsg:3857':
        gd.xform_geometry(projection)
    return gd

def get_gerrymander_metrics() -> pl.DataFrame:
    hr_elect = HrElection()
    gerry_meter = GerryMeter(hr_elect)
    metrics_df = gerry_meter.get_gerrymander_metrics()
    return metrics_df

def get_plot_df_for_metric(
        metric_df: pl.DataFrame, metric_name: str,
        party: str) -> pl.DataFrame:
    color_col_name = color_column_names[metric_name][party]
    metric_min: float = metric_df[color_col_name].min()   # type: ignore
    metric_max: float = metric_df[color_col_name].max()  # type: ignore 
    diff = metric_max - metric_min
    plot_df = metric_df.with_columns(
        ((pl.col(color_col_name) - metric_min)/diff)
           .alias('normalized_color_col')
    )
    return plot_df

def make_plotly_representation_of_metric(
        plot_df: pl.DataFrame, gd: DistrictsGeoData,
        metric_code: str, party: str, year: int) -> go.Figure:
    color_col_name = color_column_names[metric_code][party]
    red_to_blue = [(0.0, 'red'), (0.5, 'white'), (1.0, 'blue')]
    blue_to_red = [(0.0, 'blue'), (0.5, 'white'), (1.0, 'red')]

    col_names = ['State\nAbbr', color_col_name]
    columns_stack = plot_df.select(col_names).to_numpy().tolist()
    fig = go.Figure(go.Choropleth(
        geojson=gd.geojson_data,
        featureidkey='properties.GEOID',
        locations=plot_df['State\nFIPS'],
        z=plot_df['normalized_color_col'],
        colorscale=red_to_blue if party == 'Democrat' else blue_to_red,
        colorbar=dict(x=-0.15, y=0.6, len=0.4),
        customdata=columns_stack,
        hovertemplate=('<b>State: %{customdata[0]}</b><br>' +
            color_column_names[metric_code][party] + ': (%{customdata[1]:.2f}')
        ),
    )
    fig.update_geos(fitbounds='locations', visible=False)
    metric_name = re.sub('[-_]', ' ', metric_code)
    fig.update_layout(
        autosize=False,
        width=800,
        height=600,
        title=dict(
            text='U.S. House of Representatives <br>'
            + f'{year} Election {metric_name}',
            x=0.4,
            y=0.9,
            xanchor='center',
            yanchor='top',
        ),
    )
    return fig

if __name__ == '__main__':
    metric_df = get_gerrymander_metrics()
    skew_df = get_plot_df_for_metric(metric_df, 'partisan_skew', 'Democrat')
    gd = get_districts_geodata(
        '../../map-data-census/tl_2024_us_state.shp', 'epsg:4269')
    fig = make_plotly_representation_of_metric(
        metric_df, gd, 'partisan_skew', 'Democrat', 2024)
    with open('../../out/skew.html', 'w') as fh:
        fh.write(fig.to_html(full_html=True))