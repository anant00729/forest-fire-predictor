from dash import html, dcc
from dash.dependencies import Input,Output
import plotly.express as px
import pandas as pd
import geopandas as gpd
from app import app

from apps.avickars.data import polygon, forest_assessment_data

# Creating the polygons that will be plotted
# Doing it here since it only needs to be done once



# Component render
forest_data_assessment_title = html.H1('Forest Data Assessment', className='tab-headers')

forest_data_assessment_plot = dcc.Graph(id='forest-data-assessment-plot', style={'layout.autosize': 'true', 'width': '100%', 'height': '850px'})

layout = html.Div([forest_data_assessment_title, forest_data_assessment_plot ])


@app.callback(Output(component_id="forest-data-assessment-plot", component_property="figure"),
              Input(component_id="forest-data-assessment-plot", component_property="figure"))
def render_page_content(pathname):
    return forest_data_assessment_plot_figure(polygon, forest_assessment_data)

def forest_data_assessment_plot_figure(polygon, forest_assessment_data):
    # CITATION: https://plotly.com/python/mapbox-county-choropleth/#using-geopandas-data-frames
    # CITATION: https://stackoverflow.com/questions/65706720/plotly-how-to-assign-specific-colors-for-categories
    # CITATION: https://plotly.com/python/legend/
    # CITATION: https://stackoverflow.com/questions/59057881/python-plotly-how-to-customize-hover-template-on-with-what-information-to-show

    # Filtering the vizualization to just the date we want
    polygon = polygon.join(forest_assessment_data)

    polygon['squareID'] = polygon.index

    fig = px.choropleth_mapbox(polygon,
                               geojson=polygon.geometry,
                               locations=polygon.index,
                               mapbox_style="open-street-map",
                               center={"lat": 53.7267, "lon": -127.6476},
                               zoom=4.2,
                               opacity=0.2,
                               color='hasForestData',
                               color_discrete_map={'Has Data': 'green',
                                                   'No Data': 'red'},
                               custom_data=['squareID', 'hasForestData'])

    fig.update_layout(legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01,
        title='Forest data Assessment',
        bgcolor="lightgrey"
    ))

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="darkred",
            font_size=12
        )
    )

    hover_temp = "Partition ID: <b>%{customdata[0]}</b><br>Has Forest data:<b> %{customdata[1]}</b><br><extra></extra>"

    fig.update_traces(hovertemplate=hover_temp, hoverinfo=None)

    return fig

