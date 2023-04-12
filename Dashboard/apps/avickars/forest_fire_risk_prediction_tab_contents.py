from datetime import date
from dash import dcc, html
from datetime import datetime
import plotly.express as px
from app import app
from dash.dependencies import Input,Output

from apps.avickars.data import polygon, risk_data


# Component
forest_fire_risk_prediction_title = html.H1('Forest Fire Risk Prediction', className='tab-headers')

datePicker = dcc.DatePickerSingle(
        id='fire-risk-date-picker',
        min_date_allowed=date(2020, 1, 1),
        max_date_allowed=date(2021, 10, 10),
        initial_visible_month=date(2020, 8, 5),
        date=date(2020, 8, 5)
    )

datePickerRow = html.Div(children=[html.H2('Date:', className='row-sub-header'), datePicker], className='row')

risk_plot = dcc.Graph(id='fire-risk-plot', style={'layout.autosize': 'true', 'width': '100%', 'height': '850px'})

layout = html.Div([forest_fire_risk_prediction_title, datePickerRow, risk_plot])

# callbacks
@app.callback(Output(component_id='fire-risk-plot', component_property='figure'),
              Input(component_id='fire-risk-date-picker', component_property='date'))
def update_fire_risk_plot_date(new_date):
    # CITATION: https://dash.plotly.com/basic-callbacks
    return fire_risk_plot_figure(polygon, risk_data, new_date)

def fire_risk_plot_figure(squares, risk_data, date):
    # CITATION: https://plotly.com/python/mapbox-county-choropleth/#using-geopandas-data-frames
    # CITATION: https://stackoverflow.com/questions/65706720/plotly-how-to-assign-specific-colors-for-categories
    # CITATION: https://plotly.com/python/legend/
    # CITATION: https://stackoverflow.com/questions/59057881/python-plotly-how-to-customize-hover-template-on-with-what-information-to-show

    # Converting the date to a date object
    date = datetime.strptime(date, '%Y-%m-%d')

    # Filtering the vizualization to just the date we want
    squares = squares.join(risk_data[risk_data['date'] == date.date()].set_index('squareID'))

    squares['squareID'] = squares.index

    hover_temp = "Partition ID: <b>%{customdata[0]}</b><br>Date:<b> %{customdata[1]}</b><br>Predicted Fire Risk:<b> %{customdata[2]}</b>"

    fig = px.choropleth_mapbox(squares,
                               geojson=squares.geometry,
                               locations=squares.index,
                               mapbox_style="open-street-map",
                               center={"lat": 53.7267, "lon": -127.6476},
                               zoom=4.2,
                               opacity=0.2,
                               color='Risk',
                               color_discrete_map={'Low Risk': 'green',
                                                   'Medium Risk': 'yellow',
                                                   'High Risk': 'red'},
                               custom_data=['squareID', 'date', 'Risk'])

    fig.update_layout(legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01,
        title='Predicted Forest Fire Risk',
        bgcolor="lightgrey"
    ))

    fig.update_layout(
        hoverlabel=dict(
            bgcolor="darkred",
            font_size=12
        )
    )

    hover_temp = "Partition ID: <b>%{customdata[0]}</b><br>Date:<b> %{customdata[1]}</b><br>Predicted Fire Risk:<b> %{customdata[2]}</b><extra></extra>"

    fig.update_traces(hovertemplate=hover_temp, hoverinfo=None)

    return fig