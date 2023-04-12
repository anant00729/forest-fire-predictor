from dash import html, dcc
import plotly.express as px
import requests
import numpy as np
import plotly.offline as pyo
import plotly.graph_objs as go
import pandas as pd
from dash.dependencies import Input, Output, State, ALL, MATCH,ALLSMALLER
from app import app
import dash_daq as daq

import json

from apps.rka73.data import data as d1
from apps.rka73.utils import find_centeroid as fc

token = 'pk.eyJ1IjoiYW5hbnQwMDcyOSIsImEiOiJja3VyaTJyNzI1NTk0MnVrNjByYzRodWw0In0.YB3rNQc1YRyWSRC-0RRRNQ'
df = pd.read_csv(d1.files_to_read_dropdown[0]['value'])
all_region_list = list(df['REGION'].unique())

def render():
  return html.Div(
    children=[
      html.Div(
        className='a-q-map-title-container',
        children=[
        html.Div(
          className='a-q-map-top-dummy'
        ),
        html.Div(
          className='a-q-map-title',
          children=[html.H1("Air Quality Analysis",className='tab-headers')], style={'textAlign': 'center'}),
        html.Div(
          className='a-q-toggle',
          children=[
            daq.ToggleSwitch(
              id='map-toggle-switch',
              value=False
            ),
            html.Label(
              className='toggle-mode-title-class',
              id='toggle-mode-title'
            ),
          ]
        )

      ]),
      html.Div(
        className="air-q-main-container",
        children=[
            html.Div(
              className='air-q-dd-1-gp',
              children=[
                html.Div(
                  className="air-q-dd-1-wrapper",
                  children=[
                    html.H2(["Year and Month Range:"], style={'textAlign': 'center'}, className='row-sub-header'),
                    dcc.Dropdown(
                      className="air-q-dd-1",
                      id='air-content-and-year-range-picker-air-q-1',
                      options = d1.files_to_read_dropdown,
                      value = d1.files_to_read_dropdown[0]['value']
                    )]
                ),
                html.Div(
                  className="air-q-dd-2-wrapper",
                  children=[
                    html.H2("Region:", style={'textAlign': 'center'}, className='row-sub-header'),
                    dcc.Dropdown(
                      className="air-q-dd-2",
                      id='region-picker-air-q-1',
                      options=[{'label': a, 'value': a} for a in all_region_list],
                      value=all_region_list[0]
                    ),
                  ]
                )
              ]
            ),
          dcc.Graph(
            className="air-q-g-1",
            id='graph-air-q-1', figure={}),
          html.Div(
            id='show-avg-air-q'
          ),
        ]),
      # html.Div(
      #   className="air-q-main-container",
      #   children=[
      #     dcc.Graph(id='graph-air-q-2', figure={}),
      #     html.Div(
      #       id='show-avg-monthly-air-q-1'
      #     )
      #   ])
    ]
  )



@app.callback(Output('show-avg-air-q', 'children'),
              [Input('graph-air-q-1','selectedData')])
def selected_points_avg_yearly(selected_data):
  if selected_data is not None:
    display_data = "The average HR of "
    years = []
    avg_sum = 0
    for i in selected_data['points']:
      years.append(i['x'])
      avg_sum += i['y']
    avg = avg_sum / len(selected_data['points'])
    display_data += display_data + ", ".join(list(set(years))) + " is " + "{0:.3f}".format(avg)
    return display_data
  else:
    return ""



@app.callback(Output('graph-air-q-1', 'figure'),
              [Input('air-content-and-year-range-picker-air-q-1','value'),
              Input('region-picker-air-q-1','value'),
              Input('map-toggle-switch','value'),
               ])
def update_figure(filename, region_value, map_toggle_value):
  df = pd.read_csv(filename)

  df_region = df[df['REGION'] == region_value]
  all_years = list(df_region['YEAR'].unique()).copy()
  all_years.sort()
  # all_years = list(df['YEAR'].unique()).sort()

  annual_hr_avg = []
  # traces = []
  for i in range(0, len(all_years)):
    filtered_df = df_region[df_region['YEAR'] == all_years[i]]
    annual_hr_avg.append(filtered_df['ANNUAL1-HR_AVG-REGION'].values[0])

  point_colors = []
  for s_avg in annual_hr_avg:
    color_name = 'red'
    if s_avg < 0.5:
      color_name = 'green'
    elif 0.5 <= s_avg <= 2:
      color_name = 'yellow'
    point_colors.append(d1.colors_for_hr_avg[color_name])

  all_lat_lng_list = []
  zoom_level = 10
  for i in range(1, filtered_df.shape[0]):
    all_lat_lng_list.append([filtered_df['LAT'].values[i],filtered_df['LONG'].values[i]])
  center_lat_lng = fc.average_cord(all_lat_lng_list)

  if len(all_lat_lng_list) > 8:
    zoom_level = 7

  fig = go.Figure(
    go.Scattergeo(
      lon=filtered_df['LONG'],
      lat=filtered_df['LAT'],
      text=df['STATION_NAME'],
      mode='markers',
      name='mylines',
      # fillcolor="#ffe1e7",
      # fill='tozeroy',
      marker=dict(
        color=point_colors,
        size=15,
      ),
      line=dict(
        color="#4da7ff",
        # width=5
      )
    )
  )

  def filter_title_func(d):
    if d['value'] == filename:
      return True
    return False

  filtered_title = filter(filter_title_func, d1.files_to_read_dropdown)
  year_range_title = list(filtered_title)[0]['label']

  title = f'Location of stations that captured Annual HR Average (Region: {region_value}, {year_range_title})'

  fig.add_trace(go.Scattermapbox(
    lat=filtered_df['LAT'],
    lon=filtered_df['LONG'],
    mode='markers',
    marker=go.scattermapbox.Marker(
      size=17,
      color='rgb(255, 0, 0)',
      opacity=0.7
    ),
    text="Hello",
    hoverinfo='text'
  ))

  fig.add_trace(go.Scattermapbox(
    lat=filtered_df['LAT'],
    lon=filtered_df['LONG'],
    mode='markers',
    marker=go.scattermapbox.Marker(
      size=8,
      color='rgb(242, 177, 172)',
      opacity=0.7
    ),
    hoverinfo='none'
  ))

  style_mode = 'light'
  if map_toggle_value:
    style_mode = 'dark'

  fig.update_layout(
    title=title,
    title_x=0.5,
    title_font_color='darkred',
    autosize=True,
    hovermode='closest',
    showlegend=False,
    height=800,
    mapbox=dict(
      accesstoken=token,
      bearing=0,
      center=dict(
        lat=center_lat_lng[0],
        lon=center_lat_lng[1]
      ),
      pitch=0,
      zoom=zoom_level,
      style=style_mode
    )
  )
  return fig

@app.callback(Output('toggle-mode-title', 'children'),
              [Input('map-toggle-switch','value'),
               ])
def update_toggle_title(mode):
  value = "Light mode"
  if mode:
    value = "Night mode"
  return value

layout = html.Div([render()])
