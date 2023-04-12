import plotly.express as px
from dash import html, dcc
from dash.dependencies import Input, Output

from app import app
from apps.avickars.data import seven_day_weather_average, fourteen_day_weather_average

n_day_weather_average_title = html.H1('N Day TMAX Average', className='tab-headers', id='n-day-tmax-average')

n_day_weather_average_plot = dcc.Graph(id='n-day-weather-average-plot', style={'layout.autosize': 'true', 'width': '100%', 'height': '850px'})

n_day_dropdown = html.Div(dcc.Dropdown(
    id='n-day-dropdown',
    options=[
        {'label': '7 Day TMAX Average', 'value': 'seven'},
        {'label': '14 Day TMAX Average', 'value': 'fourteen'}
    ],
    value='seven'
), style={'width': '20%'})


layout = html.Div([n_day_weather_average_title, n_day_dropdown, n_day_weather_average_plot])


@app.callback(
    Output(component_id='n-day-weather-average-plot', component_property='figure'),
    Input(component_id='n-day-dropdown', component_property='value')
)
def update_output(value):
    if value == 'seven':
        return n_day_dropdown_plot_figure(seven_day_weather_average, 7)
    else:
        return n_day_dropdown_plot_figure(fourteen_day_weather_average, 14)

def n_day_dropdown_plot_figure(data, n):
    fig = px.histogram(data, x="TMAX")
    fig.update_traces(xbins=dict(  # bins used for histogram
        start=0.0,
        end=40.0,
        size=1
    ), marker={'color': 'darkred'})

    hover_temp = f"{n}" + " Day Average TMAX: <b>%{x}</b><br>Count: <b>%{y}</b>"

    fig.update_traces(hovertemplate=hover_temp)

    return fig

@app.callback(
    Output(component_id='n-day-tmax-average', component_property='children'),
    Input(component_id='n-day-dropdown', component_property='value')
)
def update_output(value):
    if value == 'seven':
        return f"7 Day TMAX Average"
    else:
        return f"14 Day TMAX Average"
