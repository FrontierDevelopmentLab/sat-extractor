import json
import math

import dash
import dash_bootstrap_components as dbc
import yaml
from dash import html
from dash.dependencies import ALL
from dash.dependencies import Input
from dash.dependencies import MATCH
from dash.dependencies import Output
from dash.dependencies import State
from dash.exceptions import PreventUpdate
from satextractor.monitors import monitors

cfg = yaml.load(open("./conf/config.yaml", "r"), Loader=yaml.SafeLoader)
cfg_cloud = yaml.load(open("./conf/cloud/gcp.yaml", "r"), Loader=yaml.SafeLoader)
cfg_monitor = yaml.load(open("./conf/monitor/gcp.yaml", "r"), Loader=yaml.SafeLoader)

monitor_api = monitors[cfg_monitor["_target_"]](**cfg_cloud, **cfg_monitor)


def tail(f, lines=20):
    total_lines_wanted = lines

    BLOCK_SIZE = 1024
    f.seek(0, 2)
    block_end_byte = f.tell()
    lines_to_go = total_lines_wanted
    block_number = -1
    blocks = []
    while lines_to_go > 0 and block_end_byte > 0:
        if block_end_byte - BLOCK_SIZE > 0:
            f.seek(block_number * BLOCK_SIZE, 2)
            blocks.append(f.read(BLOCK_SIZE))
        else:
            f.seek(0, 0)
            blocks.append(f.read(block_end_byte))
        lines_found = blocks[-1].count(b"\n")
        lines_to_go -= lines_found
        block_end_byte -= BLOCK_SIZE
        block_number -= 1
    all_read_text = b"".join(reversed(blocks))
    return b"\n".join(all_read_text.splitlines()[-total_lines_wanted:])


def build_row(job_df, job_id, row, total_tasks):
    def try_get(row, key):
        try:
            return row[key]
        except Exception:
            return ""

    def format_int(val):
        if math.isnan(val):
            return "nan"
        elif isinstance(val, int):
            return f"{val:d}"
        elif isinstance(val, float):
            return f"{int(val):d}"
        else:
            return str(val)

    elements = [html.Td(job_id, className="job-table-item")]

    for key in ["dataset_name", "task_type", "constellation"]:
        elements.append(html.Td(job_df.loc[job_id, key], className="job-table-item"))

    for key in ["STARTED", "FINISHED", "FAILED"]:
        elements.append(
            html.Td(
                format_int(row[key]) + " / " + format_int(total_tasks),
                className="job-table-item",
            ),
        )

    button_div = html.Td(
        dbc.Row(
            [
                html.Div(
                    dbc.Button(
                        children=[html.I(className="fas fa-redo-alt")],
                        id={"type": "rerun-btn", "index": job_id},
                    ),
                    id=f"tooltip-wrapper-rerun-{job_id}",
                ),
                html.Div(
                    dbc.Button(
                        children=[html.I(className="fas fa-exclamation-triangle")],
                        id={"type": "stacktrace-btn", "index": job_id},
                    ),
                    id=f"tooltip-wrapper-stacktrace-{job_id}",
                ),
            ],
        ),
        className="job-table-item",
    )

    elements.append(button_div)

    return html.Tr(elements)


def build_table(job_df, count_df, total_tasks_df):

    rows = [
        build_row(job_df, idx, row, total_tasks_df.loc[idx, "count"])
        for idx, row in count_df.iterrows()
    ]

    header = html.Tr(
        [
            html.Th(h, className="job-table-item")
            for h in [
                "job_id",
                "dataset_name",
                "task_type",
                "constellation",
                "STATUS:STARTED",
                "STATUS:FINISHED",
                "STATUS:FAILED",
                "action",
            ]
        ],
    )

    tooltips = []
    for idx, row in count_df.iterrows():
        tooltips.append(
            dbc.Tooltip(
                "rerun failed tasks",
                target=f"tooltip-wrapper-rerun-{idx}",
                placement="top",
            ),
        )
        tooltips.append(
            dbc.Tooltip(
                "query stacktraces",
                target=f"tooltip-wrapper-stacktrace-{idx}",
                placement="top",
            ),
        )

    return html.Div(
        [html.Table([header] + rows, id="job-table", className="job-table")] + tooltips,
    )


def build_collapse_stacktrace(task_id, st):

    row = dbc.Row(
        [
            dbc.Col(
                [
                    html.A(task_id, id={"type": "stacktrace-button", "index": task_id}),
                    dbc.Collapse(
                        dbc.Card(dbc.CardBody(st)),
                        id={"type": "stacktrace-collapse", "index": task_id},
                        is_open=False,
                    ),
                ],
            ),
        ],
    )

    return row


def register_callbacks(dashapp):
    """Register callbacks to the dashapp:
    populate logs, populate jobs-table, populate stacktrace, rerun jobs"""

    @dashapp.callback(
        Output("logs-div", "children"),
        Input("logging-interval", "n_intervals"),
    )
    def populate_logs(n):

        lines = tail(open(cfg["log_path"], "rb"), lines=5)

        return [dbc.Row(html.Span(el)) for el in lines.decode("utf-8").split("\n")]

    @dashapp.callback(
        Output("jobs-div", "children"),
        Input("table-interval", "n_intervals"),
    )
    def populate_jobs_table(n):

        job_df = monitor_api.get_job_parameters().set_index("job_id")

        count_df = monitor_api.get_current_tasks_status()

        count_df = count_df.set_index(["job_id", "msg_type"]).unstack()
        count_df.columns = count_df.columns.droplevel(0)

        for key in ["PARAMS", "STARTED", "FINISHED", "FAILED"]:
            if key not in count_df:
                count_df[key] = math.nan

        total_tasks_df = monitor_api.get_total_tasks_by_job().set_index("job_id")

        return build_table(job_df, count_df, total_tasks_df)

    @dashapp.callback(
        Output({"type": "stacktrace-collapse", "index": MATCH}, "is_open"),
        Input({"type": "stacktrace-button", "index": MATCH}, "n_clicks"),
        State({"type": "stacktrace-collapse", "index": MATCH}, "is_open"),
    )
    def open_st_collapse(n, is_open):
        if n:
            return not is_open
        else:
            return is_open

    @dashapp.callback(
        Output("stacktrace-div", "children"),
        Input({"type": "stacktrace-btn", "index": ALL}, "n_clicks"),
        State("stacktrace-div", "children"),
    )
    def query_stacktraces(ns, curren_div):

        ctx = dash.callback_context

        if not ctx.triggered:
            print("stacktrace - ctx not triggered")
            raise PreventUpdate

        else:

            if ns is None:
                print("stacktrace - ns is None")
                raise PreventUpdate
            elif sum([(n is not None) for n in ns]) == 0:
                print("stacktrace - all ns are None")
                raise PreventUpdate
            else:
                job_id = json.loads(ctx.triggered[0]["prop_id"].split(".")[0])["index"]

                result = monitor_api.get_stacktraces(job_id)

                rows = [
                    build_collapse_stacktrace(task_id, st)
                    for task_id, st in result[
                        ["task_id", "msg_payload"]
                    ].values.tolist()
                ]

                print("stacktrace - built rows")

                return rows

    @dashapp.callback(
        [Output("rerun-alert", "is_open"), Output("rerun-alert", "children")],
        Input({"type": "rerun-btn", "index": ALL}, "n_clicks"),
    )
    def cb_rerun_failed_tasks(ns):

        ctx = dash.callback_context

        if not ctx.triggered:
            print("rerun - not triggered")
            return [False, ""]

        elif ns is None:
            print("rerun - ns is None")
            return [False, ""]

        elif sum([(n is not None) for n in ns]) == 0:
            print("rerun - all ns are None")
            return [False, ""]

        else:

            print("rerun - rerunning")

            job_id = json.loads(ctx.triggered[0]["prop_id"].split(".")[0])["index"]

            n_reruns = monitor_api.rerun_failed_tasks(job_id)

            return [True, f"Relaunched {n_reruns} failed tasks for job_id {job_id}"]
