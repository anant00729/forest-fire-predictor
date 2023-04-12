from dash import html, dcc
import plotly.express as px
import requests
import numpy as np
import plotly.offline as pyo
import plotly.graph_objs as go
import pandas as pd
from dash.dependencies import Input, Output, State, ALL, MATCH, ALLSMALLER
from app import app
import dash

import json

from apps.rka73.data import data as d1
from apps.rka73.data.data import files_to_read_dropdown, colors_for_hr_avg, months
from apps.rka73.utils import call_api_from_rds as api

start_file_name = d1.files_to_read_dropdown[0]['value']


# df = pd.read_csv(start_file_name)
# 'apps/rka73/data/spark_transformed_files_with_geo_coords/CO_1980_2008_cleaned_stats.csv'
# 'apps/rka73/data/spark_transformed_files_with_geo_coords/CO_1980_2008_cleaned_stats.csv'
def call_app_callbacks():
    df = api.call_api(start_file_name)
    all_region_list = list(df['REGION'].unique())
    return [update_figure_1(start_file_name, all_region_list[0], df),
            on_year_point_click_1(None, start_file_name, all_region_list[0], df), all_region_list]


def render():
    fig1, fig2, all_region_list = call_app_callbacks()
    return html.Div(
        children=[
            html.H1(["Air Quality Analysis"], style={'textAlign': 'center'}, className='tab-headers'),
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
                                        id='air-content-and-year-range-picker',
                                        options=d1.files_to_read_dropdown,
                                        value=d1.files_to_read_dropdown[0]['value']
                                    )]
                            ),
                            html.Div(
                                className="air-q-dd-2-wrapper",
                                children=[
                                    html.H2(["Region:"], style={'textAlign': 'center'}, className='row-sub-header'),
                                    dcc.Dropdown(
                                        className="air-q-dd-2",
                                        id='region-picker',
                                        options=[{'label': a, 'value': a} for a in all_region_list],
                                        value=all_region_list[0]
                                    ),
                                ]
                            )
                        ]
                    ),

                    dcc.Loading(children=[
                        dcc.Graph(
                            className="air-q-g-1",
                            id='graph', figure=fig1),
                        html.Div(
                            id='show-avg'
                        )
                    ], color="#ff728f")
                ]),

            html.Div(
                className="air-q-main-container",
                children=[
                    dcc.Loading(children=[
                        dcc.Graph(id='graph-2', figure=fig2),
                        html.Div(
                            id='show-avg-monthly'
                        )
                    ], color="#ff728f")
                ])
        ]
    )


@app.callback(Output('show-avg', 'children'),
              [Input('graph', 'selectedData')],
              prevent_initial_call=True
              )
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


@app.callback(Output('show-avg-monthly', 'children'),
              [Input('graph-2', 'selectedData')],
              prevent_initial_call=True
              )
def selected_points_avg_monthly(selected_data):
    if selected_data is not None:
        display_data = "The average HR of "
        months = []
        avg_sum = 0
        for i in selected_data['points']:
            months.append(i['x'])
            avg_sum += i['y']
        avg = avg_sum / len(selected_data['points'])

        display_data += display_data + ", ".join(list(set(months))) + " is " + "{0:.3f}".format(avg)
        return display_data
    else:
        return ""


def show_annual_graph(df, region_value, filename):
    df_region = df[df['REGION'] == region_value]
    all_years = list(df_region['YEAR'].unique()).copy()
    all_years.sort()
    # all_years = list(df['YEAR'].unique()).sort()
    annual_hr_avg = []
    traces = []
    for i in range(0, len(all_years)):
        filtered_df = df_region[df_region['YEAR'] == all_years[i]]
        annual_hr_avg.append(filtered_df['ANNUAL1-HR_AVG-REGION'].values[0])
    point_colors = []
    for s_avg in annual_hr_avg:
        color_name = 'darkred'
        if s_avg < 0.5:
            color_name = 'green'
        elif 0.5 <= s_avg <= 2:
            color_name = 'yellow'
        point_colors.append(d1.colors_for_hr_avg[color_name])
    traces.append(
        go.Scatter(
            x=all_years,
            y=annual_hr_avg,
            opacity=0.7,
            mode='lines+markers',
            # name='mylines',
            # fillcolor="#ffe1e7",
            # fill='tozeroy',
            marker=dict(
                color=point_colors,
                size=15,
            ),
            line=dict(
                # color="#4da7ff",
                color="darkred",
                # width=5
            ),
            hovertemplate='Year: <b>%{x}</b><br>Value:<b> %{y}</b><br><extra></extra>'
        )
    )

    def filter_title_func(d):
        if d['value'] == filename:
            return True
        return False

    filtered_title = filter(filter_title_func, d1.files_to_read_dropdown)
    year_range_title = list(filtered_title)[0]['label']
    title = f'Annual Hourly (HR) Average (Region: {region_value}, {year_range_title})'
    return {'data': traces, 'layout': go.Layout(title=title, xaxis={'title': 'Year',
                                                                    'type': 'category'},
                                                yaxis={'title': 'Annual HR Average'}, font=dict(color='darkred'))}


def show_monthly_graph(df, clickData, year_range_picker, region_picker, button_id):
    df_region = df[df['REGION'] == region_picker]

    if clickData is None or button_id == 'region-picker' or button_id == 'air-content-and-year-range-picker':
        year = list(df_region['YEAR'].unique()).copy()[0]
    else:
        year = int(clickData['points'][0]['x'])

    # if df_region[df_region['YEAR'] == year].shape[0] == 0:
    #   year = list(df_region['YEAR'].unique()).copy()[0]

    single_year_df = df_region[df_region['YEAR'] == year]

    traces = []
    month_list = list(single_year_df['MONTH']).copy()
    month_names = []

    for a in month_list:
        month_names.append(d1.months[a - 1])

    month_avg_list = list(single_year_df['MONTHLY1-HR_AVG-REGION']).copy()

    point_colors = []
    for s_avg in month_avg_list:
        color_name = 'red'
        if s_avg < 0.5:
            color_name = 'green'
        elif 0.5 <= s_avg <= 2:
            color_name = 'yellow'
        point_colors.append(d1.colors_for_hr_avg[color_name])

    traces.append(
        go.Scatter(
            x=month_names,
            y=month_avg_list,
            opacity=0.7,
            mode='lines+markers',
            name='mylines',
            # fillcolor="#ffe1e7",
            # fill='tozeroy',
            marker=dict(
                color=point_colors,
                size=15,
            ),
            line=dict(
                color="darkred",
                # width=5
            ),hovertemplate='Month: <b>%{x}</b><br>Value:<b> %{y}</b><br><extra></extra>'
        ))

    def filter_title_func(d):
        if d['value'] == year_range_picker:
            return True
        return False

    filtered_title = filter(filter_title_func, d1.files_to_read_dropdown)
    year_range_title = list(filtered_title)[0]['label']

    title = f'Monthly Hourly (HR) Average for the year {year} (Region: {region_picker}, {year_range_title})'

    return {'data': traces, 'layout': go.Layout(
        title=title,
        xaxis={
            'title': 'Month',
            'type': 'category'
        },
        yaxis={'title': 'Monthly HR Average'}, font=dict(color='darkred'))}


@app.callback(Output('graph', 'figure'),
              [Input('air-content-and-year-range-picker', 'value'),
               Input('region-picker', 'value')],
              prevent_initial_call=True
              )
def update_figure(filename, region_value):
    df = api.call_api(filename)
    # df = pd.read_csv(filename)
    return show_annual_graph(df, region_value, filename)


def update_figure_1(filename, region_value, df):
    return show_annual_graph(df, region_value, filename)


@app.callback(Output('graph-2', 'figure'), [
    Input('graph', 'clickData'),
    Input('air-content-and-year-range-picker', 'value'),
    Input('region-picker', 'value')],
              prevent_initial_call=True)
def on_year_point_click(clickData, year_range_picker, region_picker):
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = 'No clicks yet'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    df = api.call_api(year_range_picker)
    # df = pd.read_csv(year_range_picker)
    return show_monthly_graph(df, clickData, year_range_picker, region_picker, button_id)


def on_year_point_click_1(clickData, year_range_picker, region_picker, df):
    return show_monthly_graph(df, clickData, year_range_picker, region_picker, 'No clicks yet')


layout = html.Div([render()])
