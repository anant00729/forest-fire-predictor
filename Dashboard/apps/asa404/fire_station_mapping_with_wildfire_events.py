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
# from apps.asa404.data import d1
import json

# counties = json.load(response)

token = 'pk.eyJ1IjoiYW5hbnQwMDcyOSIsImEiOiJja3VyaTJyNzI1NTk0MnVrNjByYzRodWw0In0.YB3rNQc1YRyWSRC-0RRRNQ'
df = pd.read_csv('apps/asa404/data/fire_stations.csv')
station_name = df.mof_fire_centre_name.unique()


@app.callback(
    Output("al-main-map-c1", "children"),
    [Input('fswal-station-radio-1', "value")])
def display_choropleth(area):
    api_requests = requests.get(
        f"https://fire-forrest-maps.herokuapp.com/v1/fire/getFireStation/{area}")
    gj = api_requests.json()
    number_of_cords = len(gj['features'][0]['geometry']['coordinates'][0])
    dfl = df.copy(deep=True)
    c_lat = float(dfl[dfl['mof_fire_centre_name'] == area]['centeroid_lat'].values[0])
    c_lng = float(dfl[dfl['mof_fire_centre_name'] == area]['centeroid_lng'].values[0])

    dfl['mof_fire_centre_name'] = [str(i).replace('Fire Centre', 'Region') for i in dfl['mof_fire_centre_name'].values]
    gj['features'][0]['properties']['MOF_FIRE_CENTRE_NAME'] = gj['features'][0]['properties']['MOF_FIRE_CENTRE_NAME'].replace('Fire Centre', 'Region')

    fig = px.choropleth_mapbox(
        dfl, geojson=gj,
        locations="mof_fire_centre_name",
        featureidkey="properties.MOF_FIRE_CENTRE_NAME",
        center={"lat": c_lat, "lon": c_lng},
        zoom=4,
        custom_data=['mof_fire_centre_name'],
        color='mof_fire_centre_name',
        color_discrete_map={'Northwest Region': 'darkred',
                            'Coastal Region': 'darkred',
                            'Prince George Region': 'darkred',
                            'Cariboo Region': 'darkred',
                            'Kamloops Region': 'darkred',
                            'Southeast Region': 'darkred'})
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        mapbox_accesstoken=token)
    fig.update_layout(showlegend=False)

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="darkred",
            font_size=12
        )
    )

    hover_temp = "Region: <b>%{customdata[0]}</b><br><extra></extra>"

    fig.update_traces(hovertemplate=hover_temp, hoverinfo=None)

    view = [
        html.Div([f"Number of Coordinates to Form Polygon: {number_of_cords}"]),
        dcc.Graph(
            id="choropleth-air-q",
            figure=fig
        )
    ]
    return view


@app.callback(
    Output("al-main-map-c2", "children"),
    [Input('fswal-station-radio-1', "value")])
def display_choropleth_2(area):
    api_requests = requests.get(f"https://fire-forrest-maps.herokuapp.com/v1/fire/getCleanedFireStation/{area}")
    gj = api_requests.json()
    number_of_cords = len(gj['features'][0]['geometry']['coordinates'][0])
    dfl = df.copy(deep=True)
    c_lat = float(dfl[dfl['mof_fire_centre_name'] == area]['centeroid_lat'].values[0])
    c_lng = float(dfl[dfl['mof_fire_centre_name'] == area]['centeroid_lng'].values[0])

    dfl['mof_fire_centre_name'] = [str(i).replace('Fire Centre', 'Region') for i in dfl['mof_fire_centre_name'].values]
    gj['features'][0]['properties']['mof_fire_centre_name'] = gj['features'][0]['properties']['mof_fire_centre_name'].replace('Fire Centre', 'Region')

    fig = px.choropleth_mapbox(
        dfl, geojson=gj,
        locations="mof_fire_centre_name", featureidkey="properties.mof_fire_centre_name",
        center={"lat": c_lat, "lon": c_lng},
        zoom=4,
        custom_data=['mof_fire_centre_name'],
        color='mof_fire_centre_name',
        color_discrete_map={'Northwest Region': 'darkred',
                            'Coastal Region': 'darkred',
                            'Prince George Region': 'darkred',
                            'Cariboo Region': 'darkred',
                            'Kamloops Region': 'darkred',
                            'Southeast Region': 'darkred'})
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        mapbox_accesstoken=token)

    fig.update_layout(showlegend=False)

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="darkred",
            font_size=12
        )
    )

    hover_temp = "Region: <b>%{customdata[0]}</b><br><extra></extra>"

    fig.update_traces(hovertemplate=hover_temp, hoverinfo=None)

    view = [
        html.Div([f"Number of Coordinates to Form Polygon: {number_of_cords}"]),
        dcc.Graph(
            id="choropleth-2-air-q", figure=fig
        )
    ]

    return view


def main_render():
    return html.Div([
        html.Div(
            children=[
                html.Div(
                    className='al-q-map-title-container',
                    children=[
                        html.Div(
                            className='a-q-map-top-dummy'
                        ),
                        html.H1(["BC Regional Polygon Coordinate Reduction using Ramer-Douglas-Peucker Algorithm"],
                                # style={'textAlign': 'left', 'marginBottom': '0px', 'font-size': '22px'},
                                className='tab-headers'),
                        html.Div(
                            className='a-q-toggle',
                            children=[]
                        )
                    ]),
                html.Div(
                    className="wfsal-top-radio-container",
                    children=[
                        html.H2("Region:",
                            className='row-sub-header'),
                        dcc.RadioItems(
                            className="fswal-station-radio",
                            id='fswal-station-radio-1',
                            options=[{'value': x, 'label': x.replace('Fire Centre', 'Region')}
                                     for x in station_name],
                            value=station_name[0]
                        ),
                    ]
                ),
                html.Div(
                    className='al-main-map-container',
                    children=[
                        html.Div(
                            className='al-main-map-c1',
                            children=[
                                dcc.Loading(children=[
                                    html.Div(
                                        className='al-main-map-c1-inner',
                                        id='al-main-map-c1',
                                        children=[]
                                    )
                                ], color="#ff728f")
                            ]),
                        html.Div(
                            className='al-main-map-c2',
                            children=[
                                dcc.Loading(children=[
                                    html.Div(
                                        className='al-main-map-c2-inner',
                                        id='al-main-map-c2',
                                        children=[]
                                    )
                                ], color="#ff728f")
                            ])
                    ]
                )

            ]
        )])


layout = main_render()
