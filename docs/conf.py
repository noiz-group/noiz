# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from datetime import date
import tomli

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(".."))

# Read pyproject.toml
with open("../pyproject.toml", "rb") as f:
    pyproject = tomli.load(f)

# Get version and authors from pyproject.toml
version = pyproject["project"]["version"]
authors = pyproject["project"]["authors"]
author = ", ".join(a["name"] for a in authors)

file_loc = os.path.split(__file__)[0]
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(file_loc), ".")))
sys.path.insert(0, os.path.abspath('../src/noiz'))

# -- Project information -----------------------------------------------------

project = "noiz"
copyright = f"Copyright 2019 -- {date.today().year}, {author}"

# The short X.Y version
version_parts = version.split('.')
version_short = f"{version_parts[0]}.{version_parts[1]}"

# The full version, including alpha/beta/rc tags
release = version

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx_paramlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.doctest",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
    'autoapi.extension',
]

# -- Options for autoapi extension ------------------------------------------

autoapi_type = 'python'
autoapi_dirs = ['../src/noiz']
autoapi_root = "content/autoapi"
autoapi_keep_files = True
autoapi_python_use_implicit_namespaces = True
autoapi_generate_api_docs = True

# -- Options for autodoc extension ------------------------------------------

autodoc_default_options = {
    'private-members': True,
}
autodoc_typehints = "description"

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.
# Using Furo theme for modern, clean documentation appearance.
html_theme = 'furo'

# Theme options for Furo
html_theme_options = {
    "source_repository": "https://gitlab.com/noiz-group/noiz",
    "source_branch": "main",
    "source_directory": "docs/",
    "footer_icons": [
        {
            "name": "GitLab",
            "url": "https://gitlab.com/noiz-group/noiz",
            "html": "",
            "class": "fa-brands fa-solid fa-gitlab",
        },
    ],
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# Additional HTML output options
html_title = f"{project} {version}"
html_show_sourcelink = True

# The suffix of source filenames.
source_suffix = '.rst'

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = 'index'

# -- Extension configuration -------------------------------------------------

# -- Options for intersphinx extension ---------------------------------------

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    "matplotlib": ("https://matplotlib.org/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "python": ("https://docs.python.org/3/", None),
    "scipy": ("https://docs.scipy.org/doc/scipy/reference/", None),
    "obspy": ("https://docs.obspy.org/", None)
}

# -- Options for todo extension ----------------------------------------------

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = True
