"""Sphinx configuration for the Python API reference.

The reference is read from the package source, not imported, so building it
needs no compiled engine: ``sphinx-build fluree-db-python/docs <out>``.
"""

from docutils import nodes
from sphinx.util.nodes import make_refnode

project = "Fluree for Python"
author = "Fluree"
copyright = "Fluree"

extensions = ["autoapi.extension", "myst_parser", "sphinx.ext.intersphinx"]

autoapi_dirs = ["../python"]
autoapi_root = "api"
autoapi_add_toctree_entry = False
autoapi_keep_files = False
autoapi_member_order = "bysource"
autoapi_options = ["members", "undoc-members", "show-inheritance", "imported-members"]
autoapi_python_class_content = "class"

intersphinx_timeout = 30
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "polars": ("https://docs.pola.rs/api/python/stable", None),
}
# Type variables and the awaitable connect() returns are not public names, and
# polars' inventory lists no DataFrame class.
nitpick_ignore = [("py:class", name) for name in ("T", "_T", "_Opening", "polars.DataFrame")]

html_theme = "furo"
html_title = "Fluree for Python"
html_baseurl = "https://fluree.github.io/db/python/"
myst_heading_anchors = 3


# Signatures name a class by the private module that defines it
# (``fluree._records.Commit``) and a type alias by its bare name (``Query``),
# and docstrings name members relative to the public API (``Ledger.validate``,
# ``keys``). The reference documents the classes under ``fluree`` and the
# aliases on the types page, so point each name there, shown unqualified.
def _public_reference(app, env, node, contnode):
    if node.get("refdomain") != "py":
        return None
    target = node.get("reftarget", "")
    name = target.rsplit(".", 1)[-1]
    if target.startswith("fluree._"):
        target = name
    label = env.get_domain("std").labels.get(f"type-{target.lower()}")
    if label:
        docname, labelid, _ = label
        return make_refnode(app.builder, node["refdoc"], docname, labelid, nodes.Text(name), name)
    candidates = [f"fluree.{target}", f"fluree.errors.{target}"]
    if node.get("py:class"):
        candidates += [f"{module}.{node['py:class']}.{target}" for module in (node.get("py:module"), "fluree")]
    objects = env.get_domain("py").objects
    for candidate in candidates:
        if candidate in objects:
            obj = objects[candidate]
            return make_refnode(app.builder, node["refdoc"], obj.docname, obj.node_id, nodes.Text(name), candidate)
    return None


def setup(app):
    app.connect("missing-reference", _public_reference)
