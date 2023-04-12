import dash
import plotly.express as px
from dash import dcc, html
from app import app

from apps.avickars.data import fire_partition_mapping

# Components
forest_fire_correlation_plot = dcc.Graph(id='fire-correlation-plot', style={'layout.autosize': 'true', 'width': '100%', 'height': '850px'})

forest_fire_correlation_title = html.H1('Forest Fire Reoccurrence', className='tab-headers')

forest_fire_correlation_plot_year_selector = html.Div(dcc.RangeSlider(
    id='forest-fire-correlation-plot-year-selector',
    min=0,
    max=520886,
    value=[0, 520886],
    pushable=1,
    step=100,
    allowCross=False
), style={'width': '25%'})

forest_fire_correlation_plot_year_selector_row = html.Div(children=[html.H2('Forest Fire Size Range (Hectares):', id='forest-fire-size-range-output', className='row-sub-header'), forest_fire_correlation_plot_year_selector], className='row')

layout = html.Div([forest_fire_correlation_title, forest_fire_correlation_plot_year_selector_row , forest_fire_correlation_plot])

# callbacks
@app.callback(
    dash.dependencies.Output(component_id='fire-correlation-plot', component_property='figure'),
    dash.dependencies.Input(component_id='forest-fire-correlation-plot-year-selector', component_property='value'))
def update_output(value):
    return forest_fire_correlation_plot_figure(fire_partition_mapping, value[0], value[1])


def forest_fire_correlation_plot_figure(fire_partition_mapping, fireSizeMin, fireSizeMax):
    fire_partition_mapping = fire_partition_mapping[fire_partition_mapping['FIRE_SIZE_HECTARES'] >= fireSizeMin]
    fire_partition_mapping = fire_partition_mapping[fire_partition_mapping['FIRE_SIZE_HECTARES'] <= fireSizeMax]

    fire_partition_mapping = fire_partition_mapping[['squareID', 'fire_year']].drop_duplicates()

    # Creating an unique index by group
    fire_partition_mapping['index'] = fire_partition_mapping.sort_values(['squareID', 'fire_year']).groupby('squareID').cumcount()

    # Creating a deep copy
    fire_partition_mapping_2 = fire_partition_mapping.copy()

    # Incrementing the index for the join below
    fire_partition_mapping_2['index'] += 1

    # Merging on the two indexs
    dataMerged = fire_partition_mapping.merge(fire_partition_mapping_2, on=['squareID', 'index'], how='inner')

    # Creating the difference to between one forest fire in a square, and the next time a square had a forest fire
    dataMerged['diff'] = dataMerged['fire_year_x'] - dataMerged['fire_year_y']

    fig = px.histogram(dataMerged, x="diff")

    fig.update_traces(xbins=dict(  # bins used for histogram
        start=0.0,
        end=60.0,
        size=1
    ), marker={'color': 'darkred'})

    fig.update_layout(
        xaxis_title="Years Between Fires",
        yaxis_title="Count",
        font=dict(
            #         size=18,
            color="darkred"
        )
    )

    hover_temp = "Years Between Fires: <b>%{x}</b><br>Count: <b>%{y}</b>"

    fig.update_traces(hovertemplate=hover_temp)

    return fig

@app.callback(
    dash.dependencies.Output(component_id='forest-fire-size-range-output', component_property='children'),
    dash.dependencies.Input(component_id='forest-fire-correlation-plot-year-selector', component_property='value'))
def update_output(value):
    return f"Forest Fire Size Range (Hectares): [Min: {value[0]}, Max: {value[1]}]"

