from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "backgroundColor": "darkred",
    "color": "white"
}

IMAGE_STYLE = {
    "position": "fixed",
    "left": 0,
    "bottom": 0
}

CONTENT_STYLE = {
    "marginLeft": "18rem",
    "marginRight": "2rem",
    "padding": "2rem 1rem",
}

BUTTON_STYLE = {
    "color": "darkred"
}

sidebar = html.Div(
    [
        html.H2("Forest Fire Risk Prediction & Analyses", className="app-title"),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink("Forest Fire Risk Prediction", href="/", active="exact", className="page-link"),
                dbc.NavLink("Forest data Assessment", href="/page-1", active="exact", className="page-link"),
                dbc.NavLink("Forest Fire Recurrence", href="/page-2", active="exact", className="page-link"),
                dbc.NavLink("Forest Fire N Day Max Temperature Average", href="/page-3", active="exact", className="page-link"),
                dbc.NavLink("Number of Forest Fires by Decade", href="/page-4", active="exact", className="page-link"),
                dbc.NavLink("Regional Polygon Coordinate Reduction", href="/page-5", active="exact", className="page-link"),
                dbc.NavLink("Air Quality", href="/page-6", active="exact", className="page-link"),
                dbc.NavLink("Air Quality Station Mapping", href="/page-7", active="exact", className="page-link"),
                dbc.NavLink("Forest Fire Centroid Mapping and Distribution", href="/page-8", active="exact", className="page-link"),
            ],
            vertical=True,
            pills=True
        ),
        html.Img(src="assets/CMPTLogo.png", style=IMAGE_STYLE)
    ],
    style=SIDEBAR_STYLE,
)

content = html.Div(id="page-content", style=CONTENT_STYLE)
layout = html.Div([dcc.Location(id="url"), sidebar, content])
