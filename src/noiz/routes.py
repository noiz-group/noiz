# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from flask import Blueprint

simple_page = Blueprint("simple_page", __name__, template_folder="templates")


@simple_page.route("/")
def hello():
    return "Hello world!"


@simple_page.route("/processingconfig")
def processingconfig():
    return "processing config datapoint"


@simple_page.route("/bum")
def ahh():
    return "bum bum"
