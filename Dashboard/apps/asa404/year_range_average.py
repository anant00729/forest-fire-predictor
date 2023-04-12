from dash import html, dcc
import plotly.express as px
import requests
import numpy as np
import plotly.offline as pyo
import plotly.graph_objs as go
import pandas as pd
from dash.dependencies import Input, Output, State, ALL, MATCH, ALLSMALLER
from app import app
import json


def year_range_component():
    return html.Div(
        children=call_api_for_year_range_avg(0))


def render_year_list(props):
    range_titles = props[0]
    if len(props[3]) == 0:
        default_value = range_titles[0]
    else:
        default_value = props[3]
    list_data = []

    index = 0
    for y in range_titles:
        obj = {
            'label': y,
            'value': y,
        }
        list_data.append(obj)
        index += 1

    return html.Div(
        className="year-list-container",
        children=[html.H2('Year Range:', className='row-sub-header'),
                  dcc.Dropdown(
                      className="year-range-dropdown-container",
                      id='year-range-dropdown',
                      options=list_data,
                      value=default_value, style={'width': '70%'}
                  )
                  ])


def render_graph(props):
    range_titles, x_axis_years, y_axis_count, default_range_title = props

    x_axis_data = []
    for y in x_axis_years:
        x_axis_data.append(f"{y}")

        fig = go.Figure(data=go.Bar(
            x=x_axis_data,
            y=y_axis_count,
            name='Bronze', marker={'color': 'darkred'},
            hovertemplate='Year: <b>%{x}</b><br>Number of Fires:<b> %{y}</b><br><extra></extra>'))
        fig.update_layout(hovermode="closest",
                          xaxis={'title': 'Year', 'type': 'category'},
                          yaxis={'title': 'Number of Forest Fires'}, font=dict(color='darkred'))

    return html.Div(
        className="graph-container",
        children=[
            html.Div(dcc.Graph(figure=fig,
                               id='feature-graphic'
                               ))])


def render(props):
    min_year = int(min(props[1]))
    max_year = int(max(props[1]))
    return ([
        html.Div(
            id='main-g1',
            className="ff1-wrapper", children=
            [
                html.H1(["Number of Forest Fire Events by Decade"], className='tab-headers'),
                render_year_list(props), render_graph(props),
                # dcc.RangeSlider(
                #     id='range-slider-g1',
                #     min=min_year,
                #     max=max_year,
                #     marks={i: str(i) for i in props[1]},
                #     value=[min_year, max_year]
                # ),
            ]
        )
    ])


def call_api_for_year_range_avg(selectedIndex):
    if selectedIndex == 0:
        api_requests = requests.get("https://fire-forrest-maps.herokuapp.com/v1/fire/findFireHistoryNew/123")
    else:
        api_requests = requests.get(f"https://fire-forrest-maps.herokuapp.com/v1/fire/findFireHistoryNew/{selectedIndex}")
    res_data = api_requests.json()

    range_titles = res_data['rangeTitles']
    x_axis_years = res_data['x_axis_years']
    y_axis_count = res_data['y_axis_count']

    props = [range_titles, x_axis_years, y_axis_count, '']
    return render(props)


# @app.callback(Output('intermediate-value', 'data'), Input('all-data-g1', 'value'))
# def clean_data(value):
#   print(value)
#   return json.dumps(value)

@app.callback(
    Output(component_id='main-g1', component_property='children'),
    [Input(component_id='year-range-dropdown', component_property='value'),
     # Input(component_id='range-slider-g1', component_property='value')
     ]
)
def handle_year_select(value):
    # min_year, max_year = slider_values
    weather_requests = requests.get(
        f"https://fire-forrest-maps.herokuapp.com/v1/fire/findFireHistoryNew/{value}")
    res_data = weather_requests.json()

    x_axis_years = res_data['x_axis_years']
    y_axis_count = res_data['y_axis_count']
    range_titles = res_data['rangeTitles']

    year_ranges_min = int(value.split(' - ')[0])
    if value.split(' - ')[1] == 'Present':
        year_ranges_max = 2021
    else:
        year_ranges_max = int(value.split(' - ')[1])

    # isInRangeForMin = min_year >= year_ranges_min and min_year <= year_ranges_max
    # isInRangeForMax = max_year >= year_ranges_min and max_year <= year_ranges_max
    # isInRange = isInRangeForMin and isInRangeForMax

    # if isInRange:
    #     min_index = x_axis_years.index(str(min_year))
    #     max_index = x_axis_years.index(str(max_year))
    #     x_axis_years = x_axis_years[min_index:max_index + 1]
    #     y_axis_count = y_axis_count[min_index:max_index + 1]
    #     r_min_year = int(min(x_axis_years))
    #     r_max_year = int(max(x_axis_years))
    # else:
    #     r_min_year = int(min(res_data['x_axis_years']))
    #     r_max_year = int(max(res_data['x_axis_years']))

    props = [range_titles, x_axis_years, y_axis_count, value]

    return [
        html.H1(["Number of Forest Fire Events by Decade"], className='tab-headers'),
        render_year_list(props), render_graph(props),
        # dcc.RangeSlider(
        #     id='range-slider-g1',
        #     min=int(min(res_data['x_axis_years'])),
        #     max=int(max(res_data['x_axis_years'])),
        #     marks={i: str(i) for i in res_data['x_axis_years']},
        #     value=[r_min_year, r_max_year]
        # ),
    ]


def fire_cause_component(year_range):
    if year_range == 0:
        api_req = requests.get("https://fire-forrest-maps.herokuapp.com/v1/fire/firefindFireCauseVsCount/123")
    else:
        api_req = requests.get(f"https://fire-forrest-maps.herokuapp.com/v1/fire/firefindFireCauseVsCount/{year_range}")
    res_data = api_req.json()

    # range_titles = res_data['rangeTitles']
    # years_list = res_data['years']
    # person_count_list = res_data['person_count']
    # lightening_count_list = res_data['lightening_count']
    # y_axis_count = res_data['y_axis_count']
    return render_fire_cause(res_data)


def render_fire_cause(res_data):
    min_year = int(min(res_data['years']))
    max_year = int(max(res_data['years']))
    return html.Div(
        children=[
            html.Div(
                id='main-g2',
                className="ff1-wrapper", children=
                [
                    html.H1(["Number of Fires due to Lightning v. Person By Decade"], className='tab-headers'),
                    render_year_list_fire_cause(res_data), render_graph_fire_cause(res_data),
                    # dcc.RangeSlider(
                    #     id='range-slider-g2',
                    #     min=min_year,
                    #     max=max_year,
                    #     marks={i: str(i) for i in res_data['years']},
                    #     value=[min_year, max_year]
                    # ),
                ]
            )
        ])


def render_year_list_fire_cause(res_data):
    range_titles = res_data['rangeTitles']
    list_data = []

    if "dropDownDefaultValue" not in res_data:
        res_data['dropDownDefaultValue'] = range_titles[0]

    index = 0
    for y in range_titles:
        obj = {
            'label': y,
            'value': y,
        }
        list_data.append(obj)
        index += 1

    return html.Div(
        className="year-list-container",
        children=[html.H2('Year Range:', className='row-sub-header'),
                  dcc.Dropdown(
                      className="year-range-dropdown-container",
                      id='year-range-dropdown-1',
                      options=list_data,
                      value=res_data['dropDownDefaultValue'], style={'width': '70%'}
                  )
                  ])


def render_graph_fire_cause(res_data):
    # years_list = res_data['years']
    # person_count_list = res_data['person_count']
    # lightening_count_list = res_data['lightening_count']

    fig = go.Figure([go.Bar(
        x=res_data['years'],
        y=res_data['person_count'],
        name='Due to Person', marker={'color': 'darkred'},
        width=0.2, hovertemplate='Year ID: <b>%{x}</b><br>Value:<b> %{y}</b><br><extra></extra>'

    ),
        go.Bar(
            x=res_data['years'],
            y=res_data['lightening_count'],
            name='Due to Lightning', marker={'color': 'grey'},
            width=0.2, hovertemplate='Year: <b>%{x}</b><br>Number of Fires:<b> %{y}</b><br><extra></extra>'
        )])
    fig.update_layout(hovermode="closest",
                      xaxis={'title': 'Year', 'type': 'category'},
                      yaxis={'title': 'Number of Forest Fires'},
                      font=dict(color='darkred'))

    return html.Div(
        className="graph-container",
        children=[
            html.Div(dcc.Graph(
                id='feature-graphic-1',
                figure=fig
            ))])


@app.callback(
    Output(component_id='main-g2', component_property='children'),
    [Input(component_id='year-range-dropdown-1', component_property='value'),
     # Input(component_id='range-slider-g2', component_property='value')
     ],
)
def handle_year_select_fire_cause(value):
    # min_year, max_year = slider_values
    api_req = requests.get(f"https://fire-forrest-maps.herokuapp.com/v1/fire/firefindFireCauseVsCount/{value}")
    res_data = api_req.json()

    x_axis_years = res_data['years']
    y1_axis_count = res_data['person_count']
    y2_axis_count = res_data['lightening_count']
    range_titles = res_data['rangeTitles']

    year_ranges_min = int(value.split(' - ')[0])
    if value.split(' - ')[1] == 'Present':
        year_ranges_max = 2021
    else:
        year_ranges_max = int(value.split(' - ')[1])

    # isInRangeForMin = min_year >= year_ranges_min and min_year <= year_ranges_max
    # isInRangeForMax = max_year >= year_ranges_min and max_year <= year_ranges_max
    # isInRange = isInRangeForMin and isInRangeForMax

    # if isInRange:
    #     min_index = x_axis_years.index(min_year)
    #     max_index = x_axis_years.index(max_year)
    #     x_axis_years = x_axis_years[min_index:max_index + 1]
    #     y1_axis_count = y1_axis_count[min_index:max_index + 1]
    #     y2_axis_count = y2_axis_count[min_index:max_index + 1]
    #     r_min_year = int(min(x_axis_years))
    #     r_max_year = int(max(x_axis_years))
    # else:
    #     r_min_year = int(min(res_data['years']))
    #     r_max_year = int(max(res_data['years']))

    all_years = list(res_data['years']).copy()
    res_data['years'] = x_axis_years
    res_data['person_count'] = y1_axis_count
    res_data['lightening_count'] = y2_axis_count
    res_data['rangeTitles'] = range_titles
    res_data['dropDownDefaultValue'] = value

    return [
        html.H1(["Number of Fires due to Lightning v. Person By Decade"], className='tab-headers'),
        render_year_list_fire_cause(res_data), render_graph_fire_cause(res_data),
        # dcc.RangeSlider(
        #     id='range-slider-g2',
        #     min=int(min(all_years)),
        #     max=int(max(all_years)),
        #     marks={i: str(i) for i in all_years},
        #     value=[r_min_year, r_max_year]
        # ),
    ]


layout = html.Div([year_range_component(), fire_cause_component(0)])
