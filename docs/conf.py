import stanford_theme
import os
import sys
sys.path.insert(0, os.path.abspath('..'))

extensions = [
    'sphinx.ext.napoleon',
    #'sphinx.ext.autodoc',
    'sphinx.ext.viewcode',
    'sphinx.ext.mathjax',
    'sphinx.ext.intersphinx',
]



# The name of the entry point, without the ".rst" extension.
# By convention this will be "index"
master_doc = "index"
# This values are all used in the generated documentation.
# Usually, the release and version are the same,
# but sometimes we want to have the release have an "rc" tag.
project = "c3x-data"
autodoc_typehints = "none"
#napoleon_use_rtype = False
copyright = "2021, BSGIP"
author = "BSGIP"
version = release = "2021.1.0"
html_theme = "stanford_theme"
html_theme_path = [stanford_theme.get_html_theme_path()]






