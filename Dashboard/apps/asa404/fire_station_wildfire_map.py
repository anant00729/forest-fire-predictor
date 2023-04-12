from dash import html, dcc
import plotly.express as px
import requests
import numpy as np
import plotly.offline as pyo
import plotly.graph_objs as go
import pandas as pd
from dash.dependencies import Input, Output, State, ALL, MATCH, ALLSMALLER
from app import app
import dash_daq as daq

import json

from apps.asa404.data import data as d1

token = 'pk.eyJ1IjoiYW5hbnQwMDcyOSIsImEiOiJja3VyaTJyNzI1NTk0MnVrNjByYzRodWw0In0.YB3rNQc1YRyWSRC-0RRRNQ'
file_base_path = 'apps/asa404/data/map_corelation_data.csv'

df = pd.read_csv(file_base_path)
all_years = list(df['fire_year'].unique())
all_years.sort()
all_stations = list(df['mof_fire_centre_name'].unique())


def render():
    start_year = int(all_years[0])
    end_year = int(all_years[len(all_years) - 1])
    return html.Div(
        className="fsw-main-body-con",
        children=[
            html.Div(
                className='a-q-map-title-container',
                children=[
                    html.Div(
                        className='a-q-map-top-dummy'
                    ),
                    html.H1(["Forest Forests with Individual Regions in BC"],
                            style={'textAlign': 'center', 'marginBottom': '0px', 'font-size': '26px'}, className='tab-headers'),
                    html.Div(
                        className='a-q-toggle',
                        children=[
                            daq.ToggleSwitch(
                                id='wf-map-toggle-switch',
                                value=True
                            ),
                            html.Label(
                                className='toggle-mode-title-class',
                                id='wf-toggle-mode-title'
                            ),
                        ]
                    )

                ]),

            html.Div(
                className='wfs-slider-container',
                children=[

                    html.Div(
                        className="wfs-slider-container-inner",
                        children=[
                            html.H2(
                                className='row-sub-header',
                                children=["Year Range:"]),
                            dcc.RangeSlider(
                                className='wfs-range-slider-map',
                                id='wf-map-year-slider',
                                min=start_year,
                                max=end_year,
                                step=1,
                                value=[2006, 2006]
                            ),
                            html.Div(
                                className='wfs-slider-op-container',
                                children=[
                                    html.Div(
                                        className='wfs-slider-op',
                                        id='wfs-min-year',
                                        children=['Hello all']),
                                    html.Div(
                                        className='wfs-slider-op',
                                        id='wfs-max-year',
                                        children=['Hello all']),
                                ]
                            )
                        ]),

                    html.Div(
                        className="wfs-top-radio-container",
                        children=[
                            html.H2(
                                className='row-sub-header',
                                children=["Region:"]),
                            dcc.RadioItems(
                                className="fsw-station-radio",
                                id='fsw-station-radio-1',
                                options=[{'value': x, 'label': x.replace("Fire Centre", 'Region')}
                                         for x in all_stations],
                                value=all_stations[0]
                            ),
                        ]
                    )
                ]
            ),

            html.Div(
                className='fsw-map-container',
                children=[
                    html.Div(
                        className='fsw-map-radio-gp',
                        children=[
                            dcc.Graph(id="fsw-box-plot", figure={}),
                        ]
                    ),
                    html.Div(
                        className='fsw-map-gp',
                        id='fswm-map-view',
                        children=[]
                    )

                ]
            )
        ]
    )


@app.callback(Output('wf-toggle-mode-title', 'children'),
              [Input('wf-map-toggle-switch', 'value'),
               ])
def update_toggle_title(mode):
    value = "Light mode"
    if mode:
        value = "Night mode"
    return value


@app.callback(
    Output('wfs-min-year', 'children'),
    Input('wf-map-year-slider', 'value'))
def update_output(value):
    return 'Start Year: "{}"'.format(value[0])


@app.callback(
    Output('wfs-max-year', 'children'),
    Input('wf-map-year-slider', 'value'))
def update_output(value):
    return 'End Year: "{}"'.format(value[1])


@app.callback(
    Output('fsw-box-plot', 'figure'),
    [Input('wf-map-year-slider', 'value'),
     Input('fsw-station-radio-1', 'value')])
def update_box_plot_hectares(year_range, station_name):
    start_year, end_year = year_range
    df = pd.read_csv(file_base_path)
    year_r_df = df[(start_year <= df['fire_year']) & (df['fire_year'] <= end_year)]
    sta_df = year_r_df[station_name == year_r_df['mof_fire_centre_name']]

    fig = px.box(sta_df, x="fire_cause", y="fire_size_hectares", height=750)
    fig.update_layout(
        xaxis_title="Fire Cause",
        yaxis_title="Fire Size (hectares)",
        margin={"b": 0}, font=dict(color='darkred')
    )
    fig.update_xaxes(title_font_color='darkred')
    fig.update_traces(marker={'color': 'darkred'})
    return fig


@app.callback(
    Output('fswm-map-view', 'children'),
    [Input('wf-map-year-slider', 'value'),
     Input('fsw-station-radio-1', 'value'),
     Input('wf-map-toggle-switch', 'value')])
def update_map(year_range, station_name, map_toggle_value):
    start_year, end_year = year_range
    df = pd.read_csv(file_base_path)
    year_r_df = df[(start_year <= df['fire_year']) & (df['fire_year'] <= end_year)]
    sta_df = year_r_df[station_name == year_r_df['mof_fire_centre_name']]

    station_location = d1.get_location_from_station_name(station_name)
    number_of_wfe = sta_df.shape[0]

    fig = go.Figure(
        go.Scattergeo(
            lat=sta_df['centeroid_lat'],
            lon=sta_df['centeroid_lng'],
            text=df['fire_size_hectares'],
            mode='markers',
            name='mylines',
        )
    )

    title = f'{number_of_wfe} Wildfire events occurred between {start_year} and {end_year} around {station_name}'

    marker_colors = []
    for i in sta_df['fire_cause'].values:
        if i == 'Lightning':
            marker_colors.append("#ff728f")
        elif i == 'Person':
            marker_colors.append("#FBBF24")
        else:
            marker_colors.append("#0096d4")

    fig.add_trace(go.Scattermapbox(
        lat=sta_df['centeroid_lat'],
        lon=sta_df['centeroid_lng'],
        # fillcolor=sta_df['fire_year'],
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=17,
            color=marker_colors,
            opacity=0.7,
            # coloraxis=sta_df['fire_cause']
        ),
        text="Hello",
        hoverinfo='text'
    ))

    fig.add_trace(go.Scattermapbox(
        lat=sta_df['centeroid_lat'],
        lon=sta_df['centeroid_lng'],
        # fillcolor="maroon",
        mode='markers',
        marker=go.scattermapbox.Marker(
            size=8,
            # color='rgb(242, 177, 172)',
            color=marker_colors,
            opacity=0.7,
            # coloraxis=sta_df['fire_cause']
        ),
        hoverinfo='none'
    ))

    style_mode = 'light'
    if map_toggle_value:
        style_mode = 'dark'

    fig.update_layout(
        title=title,
        title_x=0.5,
        autosize=True,
        # margin=dict(t=30, b=30, l=30, r=30),
        margin=dict(t=0, b=0, l=0, r=0),
        hovermode='closest',
        showlegend=False,
        height=800,
        mapbox=dict(
            accesstoken=token,
            bearing=0,
            center=dict(
                lat=station_location['lat'],
                lon=station_location['long']
            ),
            pitch=0,
            zoom=station_location['zoom'],
            style=style_mode,
        ),
    )
    # fig.update_layout(showlegend=True)

    fire_cause_counts = sta_df.groupby('fire_cause')['fire_cause'].count()
    dict_fire_cause = dict(fire_cause_counts)

    label_view = []

    for i in dict_fire_cause.keys():
        if i == "Lightning":
            name = 'dot-red'
        elif i == "Person":
            name = 'dot-yellow'
        else:
            name = 'dot-blue'
        label_view.append(
            html.Div(
                className='legend-and-event-count',
                children=[
                    html.Div(
                        className=name
                    ),
                    html.Label(
                        [f"{i} (Event Count: {dict_fire_cause[i]})"]
                    )
                ])
        )

    view = [
        html.Div(
            className="fswm-map-l-container",
            children=[*label_view]
        ),
        dcc.Graph(
            id='fsw-graph-1', figure=fig),
    ]
    return view


layout = html.Div([render()])
