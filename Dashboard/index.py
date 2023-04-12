import html
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
from app import app

from apps.common.sidebar import sidebar as sidebar_layout

from apps.avickars import \
    forest_fire_risk_prediction_tab_contents as p1, \
    forest_data_assessment_tab_contents as p2, \
    forest_fire_correlation_tab_contents as p3, \
    n_day_temp_average_contents as p4

from apps.asa404 import \
    year_range_average as p5, \
    fire_station_mapping_with_wildfire_events as p6, \
    fire_station_wildfire_map as p9


from apps.rka73 import \
    air_quality_yearly as p7, \
    air_quality_station_mapping as p8

CONTENT_STYLE = {
    "marginLeft": "18rem",
    "marginRight": "2rem",
    "padding": "2rem 1rem",
}

content = html.Div(id="page-content", style=CONTENT_STYLE)
app.layout = html.Div([dcc.Location(id='url', refresh=False), sidebar_layout, content])


@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/':
        return p1.layout
    elif pathname == '/page-1':
        return p2.layout
    elif pathname == '/page-2':
        return p3.layout
    elif pathname == '/page-3':
        return p4.layout
    elif pathname == '/page-4':
        return p5.layout
    elif pathname == '/page-5':
        return p6.layout
    elif pathname == '/page-6':
        return p7.layout
    elif pathname == '/page-7':
        return p8.layout
    elif pathname == '/page-8':
        return p9.layout
    else:
        return dbc.Jumbotron(
            [
                html.H1("404: Not found", className="text-danger"),
                html.Hr(),
                html.P(f"The pathname {pathname} was not recognised..."),
            ]
        )


if __name__ == '__main__':
    app.run_server(debug=False, port=2023)
