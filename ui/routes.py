from flask import Blueprint
from flask import redirect

routes = Blueprint("routes", __name__, template_folder="templates")


@routes.route("/")
@routes.route("/index")
def index():
    """Simple redirect to the dashboard"""
    return redirect("dashboard")
