from flask import Blueprint

simple_page = Blueprint("simple_page", __name__, template_folder="templates")

# a simple page that says hello
@simple_page.route("/")
def hello():
    return "Twoja stara tanczy na pomaranczy i twoj stary tez"


@simple_page.route("/processingconfig")
def processingconfig():
    return "Twoja  kreci lody"


@simple_page.route("/wut")
def ahh():
    return "wutwut"
