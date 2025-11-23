from typing import Optional
import re
import os
from datetime import datetime
import streamlit as st
import polars as pl
import hrelectviz.hrelection as hre
from hrelectviz.gerrymeter import shorten_column_name, gm_column_names
from scripts.gerrymander_metrics_plotly import (
    get_gerrymander_metrics, get_districts_geodata, get_plot_df_for_metric,
    make_plotly_representation_of_metric
)

os.environ['LANG'] = 'en_US.UTF-8'
os.environ['LC_ALL'] = 'en_US.UTF-8'


@st.cache_data
def load_data(columns: Optional[list[str]] = None) -> pl.DataFrame:
    metric_df = get_gerrymander_metrics()
    if columns:
        metric_df = metric_df.select(columns)
    return metric_df


if __name__ == '__main__':
    st.markdown('''
        <style>
            .block-container {
                 padding-top: 2rem; padding-bottom: 0rem;
                 padding-left: 0rem; padding-right: 0rem;
                 text-align: center
            }
            .stDataFrame thead th {
                white-space: pre-line !important;
            }  
        </style>
    ''',
        unsafe_allow_html=True,
    )

    st.set_page_config(layout='wide')
    with st.sidebar:
        year = st.number_input('Election year:',
            value=hre.get_most_recent_house_election_year()
        )
        if year % 2 == 1 or year < 2010 or year > datetime.now().year:
            st.error('Please enter an even-numbered past year')
            st.stop()
        metric_name = st.radio(
            'Choose metric to map:',
            options=[
                'partisan skew', 'efficiency gap', 'mean-median difference',
            ],
            index=0
        )
        metric_code = re.sub('[- ]', '_', metric_name)
        party = st.radio(
            'Major Party',
            options=['Democrat', 'Republican'],
            index=0
        )
    st.html(f'<h3>Gerrymandering: {year} U.S. House Elections<br>'
            f'<h3>Lower 48 map of {metric_name} metric</h3>')
    col1, col2 = st.columns(2)
    metric_df = load_data()
    plot_df = get_plot_df_for_metric(metric_df, metric_code, party)
    gd = get_districts_geodata(
        './map-data-census/tl_2024_us_state.shp', 'epsg:4269')
    fig = make_plotly_representation_of_metric(
        plot_df, gd, metric_code, party, year)

    with col1:
        colnames = gm_column_names[metric_code]
        col_config = {
            'State Abbr': st.column_config.Column('State', pinned=True)
        }
        st.dataframe(
            metric_df.select(colnames).rename(shorten_column_name),
            column_config=col_config,
        )
    with col2:
        st.plotly_chart(fig)