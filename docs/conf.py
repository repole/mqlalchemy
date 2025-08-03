# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import os
import sys
root_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, root_path)
import mqlalchemy

project = 'MQLAlchemy'
copyright = '2025, Nicholas Repole'
author = 'Nicholas Repole'
# The short X.Y version.
version = ".".join(mqlalchemy.__version__.split(".")[0:2])
# The full version, including alpha/beta/rc tags.
release = mqlalchemy.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc', 
    'sphinx.ext.intersphinx', 
    'sphinx.ext.coverage', 
    'sphinx.ext.viewcode', 
]
templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# autodoc options
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# autosummary options
autosummary_generate = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'collapse_navigation': False,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False,
    'style_external_links': True,
}
html_static_path = ['_static']
