import os
import sys
from datetime import datetime


sys.path.insert(0, os.path.abspath("../../"))

from deirokay.__version__ import __version__  # noqa: E402

# -- Project information -----------------------------------------------------

project = "Deirokay"
copyright = f"{datetime.now().year}, Big Data"
author = "Marcos Bressan"

release = __version__

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.todo",
    "sphinx.ext.napoleon",
    # Link to other project's documentation (see mapping below)
    "sphinx.ext.intersphinx",
    # Add a link to the Python source code for classes, functions etc.
    "sphinx.ext.viewcode",
    # Add a link to another page of the documentation
    "sphinx.ext.autosectionlabel",
    # Theme
    "sphinx_rtd_theme",
    # Notebook support
    "nbsphinx",
]

# napoleon configuration for numpy docstring
todo_include_todos = True
todo_link_only = True
napoleon_use_ivar = True
napoleon_use_google_string = False
napoleon_include_special_with_doc = True

# autosummary
autosummary_generate = True  # Turn on sphinx.ext.autosummary
autoclass_content = "both"  # Add __init__ doc (ie. params) to class summaries
# If no class summary, inherit base class summary
autodoc_inherit_docstrings = True
# Remove 'view source code' from top of page (for html, not python)
html_show_sourcelink = False

templates_path = ["_templates"]

language = "en_US"

exclude_patterns = []
exclude_dirs = ["images", "scripts", "sandbox"]

source_suffix = ".rst"

master_doc = "index"

html_theme = "sphinx_rtd_theme"

html_static_path = ["_static", "images"]

html_theme_options = {
    "logo_only": False,
    "display_version": True,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False
}

html_logo = "images/bigdatalogo.png"
html_favicon = "images/favicon.ico"

latex_elements = {
    "papersize": "letterpaper",
    "pointsize": "10pt",
    "figure_align": "htbp",
    "preamble": r"""
\DeclareUnicodeCharacter{0301}{\'{e}}
""",
}


def setup(app):
    app.add_css_file("stylesheet.css")
