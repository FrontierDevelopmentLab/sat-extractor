import dash_bootstrap_components as dbc
from dash import dcc
from dash import html

layout = html.Div(
    children=[
        dbc.Row(
            [  # main row
                dbc.Col(
                    [  # main col
                        dbc.Row(
                            [  # logs row
                                dbc.Col(
                                    [
                                        html.H4("Logs"),
                                        dcc.Interval(
                                            id="logging-interval",
                                            interval=5 * 1000,
                                            n_intervals=0,
                                        ),
                                        html.Div(
                                            [
                                                dbc.Col(id="logs-div"),
                                            ],
                                        ),
                                    ],
                                    width=12,
                                ),
                            ],
                        ),
                        html.Hr(),
                        dbc.Alert(id="rerun-alert", is_open=False, duration=10000),
                        dbc.Row(
                            [
                                dbc.Col(
                                    [  # job status
                                        html.H4("Jobs Table"),
                                        dcc.Interval(
                                            id="table-interval",
                                            interval=10 * 1000,
                                            n_intervals=0,
                                        ),
                                        html.Div(id="jobs-div"),
                                    ],
                                    width=8,
                                ),
                                dbc.Col(
                                    [  # stacktrace
                                        html.H4("Stacktrace"),
                                        html.Div(id="stacktrace-div"),
                                    ],
                                    width=4,
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ],
    style={"padding": "20px"},
)
