import plotly.express as px  # type: ignore
import plotly.graph_objects as go   # type: ignore
import polars as pl

import hrelectviz.ushelper as ush
from hrelectviz.hrelection import HrElection
from hrelectviz.districtsgeodata import DistrictsGeoData
from hrelectviz.gerrymeter import GerryMeter

def get_skew_column_name(party: str) -> str:
    return f'Representation - Vote,\n{party}'


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

def get_skew_df() -> pl.DataFrame:
    hr_elect = HrElection()
    gerry_meter = GerryMeter(hr_elect)
    skew_df = gerry_meter.get_partisan_skew()
    print(f'{hr_elect.dfs["state_nwinners_by_party"].schema=}')
    skew_df = skew_df.filter(pl.col('State\nAbbr').is_in(ush.lower48_abbrs)
    )

    color_cols = []
    for party in ['Democrat', 'Republican']:
        skew_col = (f'Representation - Vote,\n{party}')
        skew_min = skew_df[skew_col].min()
        skew_max = skew_df[skew_col].max()
        print(f'{skew_min=}', f'{skew_max=}', type(skew_min))
        skew_mid = (skew_min + skew_max) / 2.0  # type: ignore
        print(f'{skew_mid=}')
        color_cols.append(
            (((pl.col(skew_col) - skew_min) / (skew_max - skew_min))  # type: ignore
                ).alias(f'{party}_color_col')
        )
    skew_df = skew_df.with_columns(
        *color_cols,
        pl.col('State\nAbbr').replace(ush.abbr_to_name).alias('state_name')
    )
    return skew_df

def make_plotly_representation_vote_skew(
        skew_df: pl.DataFrame, gd: DistrictsGeoData, party: str) -> go.Figure:

    red_to_blue = [(0.0, 'red'), (0.5, 'white'), (1.0, 'blue')]
    blue_to_red = [(0.0, 'blue'), (0.5, 'white'), (1.0, 'red')]
    other_col_names = [
        'State\nAbbr',
        'State Vote %\nDemocrat', 'Democrat\ndelegate\ncount',
        'Democrat\ndelegate %', 'State Vote %\nRepublican',
        'Republican\ndelegate\ncount', 'Republican\ndelegate %',
        get_skew_column_name(party)
    ]
    other_col_stack = skew_df.select(other_col_names).to_numpy().tolist()
    fig = go.Figure(go.Choropleth(
        geojson=gd.geojson_data,
        featureidkey='properties.GEOID',
        locations=skew_df['State\nFIPS'],
        z=skew_df[f'{party}_color_col'],
        colorscale=red_to_blue if party == 'Democrat' else blue_to_red,
        colorbar=dict(x=-0.15, y=0.6, len=0.4),
        customdata=other_col_stack,
        hovertemplate=('<b>State: %{customdata[0]}</b><br>' +
            'Democrats: (%{customdata[1]:.2f} %) agg. vote; ' +
            '%{customdata[2]} delegates (%{customdata[3]:.2f} %)<br>' +
            'Republicans: (%{customdata[4]:.2f} %) agg. vote; ' +
            '%{customdata[5]} delegates (%{customdata[6]:.2f} %)<br>' +
            'Skew: %{customdata[7]:.2f} %'
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
    return fig

if __name__ == '__main__':
    skew_df = get_skew_df()
    gd = get_districts_geodata(
        '../../map-data-census/tl_2024_us_state.shp', 'epsg:4269')
    fig = make_plotly_representation_vote_skew(skew_df, gd, 'Democrat')
    with open('../../out/skew.html', 'w') as fh:
        fh.write(fig.to_html(full_html=True))