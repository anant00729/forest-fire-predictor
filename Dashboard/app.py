import dash
import dash_bootstrap_components as dbc
app_dash = dash
app = app_dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
server = app.server