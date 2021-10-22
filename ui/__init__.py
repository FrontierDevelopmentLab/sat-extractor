import json
import os

import dash
import dash_auth
from flask import Flask
from flask.helpers import get_root_path

from .routes import routes


def create_app():
    """Construct core Flask application with embedded Dash app."""
    app = Flask(__name__)

    assert "USERNAME" in os.environ.keys(), "Specify USERNAME in environment variables"
    assert (
        "USERPASSWORD" in os.environ.keys()
    ), "Specify USERPASSWORD in environment variables"

    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ.keys():

        # write google credentials to file... annoying
        credentials_json = {
            "type": os.environ["type"],
            "project_id": os.environ["project_id"],
            "private_key_id": os.environ["private_key_id"],
            "private_key": os.environ["private_key"].replace("\\n", "\n"),
            "client_email": os.environ["client_email"],
            "client_id": os.environ["client_id"],
            "auth_uri": os.environ["auth_uri"],
            "token_uri": os.environ["token_uri"],
            "auth_provider_x509_cert_url": os.environ["auth_provider_x509_cert_url"],
            "client_x509_cert_url": os.environ["client_x509_cert_url"],
        }
        print(credentials_json)

        json.dump(
            credentials_json,
            open(os.path.join(os.getcwd(), "gcp-credentials.json"), "w"),
        )
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
            os.getcwd(),
            "gcp-credentials.json",
        )

    from ui.dashboard.layout import layout
    from ui.dashboard.callbacks import register_callbacks

    register_dashapp(app, "ML4CC", "dashboard", layout, register_callbacks)

    app.register_blueprint(routes, url_prefix="/")

    return app


def register_dashapp(app, title, base_pathname, layout, register_callbacks_fun):
    """Register the Dash callbacks with the app."""
    meta_viewport = {
        "name": "viewport",
        "content": "width=device-width, initial-scale=1, shrink-to-fit=no",
    }

    external_stylesheets = [
        "https://stackpath.bootstrapcdn.com/bootswatch/4.5.2/yeti/bootstrap.min.css",
        "https://use.fontawesome.com/releases/v5.10.2/css/all.css",
    ]

    my_dashapp = dash.Dash(
        __name__,
        server=app,
        url_base_pathname=f"/{base_pathname}/",
        assets_folder=get_root_path(__name__) + f"/{base_pathname}/assets/",
        external_stylesheets=external_stylesheets,
        meta_tags=[meta_viewport],
    )

    VALID_USERS = [(os.environ["USERNAME"], os.environ["USERPASSWORD"])]

    dash_auth.BasicAuth(my_dashapp, VALID_USERS)

    with app.app_context():
        my_dashapp.title = title
        my_dashapp.layout = layout
        if register_callbacks_fun:
            register_callbacks_fun(my_dashapp)
