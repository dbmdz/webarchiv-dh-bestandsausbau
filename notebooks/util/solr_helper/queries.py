import re

import pysolr


def get_facets(solr_url: str, facet_field: str):
    solr = pysolr.Solr(solr_url)
    results = solr.search(
        "*:*", "select", **{"facet": "on", "facet.field": facet_field, "rows": 0}
    )
    annotations = results.facets["facet_fields"][facet_field]
    annotations_counted = []
    for i in range(0, len(annotations), 2):
        annotations_counted.append(f"{annotations[i]} ({annotations[i+1]})")
    return annotations_counted


def format_filter_query(filter_input: [str]):
    # remove count
    filters = [re.sub(" \(\d+\)$", "", f) for f in filter_input]
    filter_query = ""
    if len(filters) > 0:
        filter_query = "src_annotations:"
    for f in filters:
        filter_query += f'"{f}" OR src_annotations:'
    filter_query = re.sub(" OR src_annotations:$", "", filter_query)
    return filter_query


def get_links(solr_url: str, query: str, filters: [str], rows: int):
    filter_query = format_filter_query(filters)
    highlight = "on"
    if query.strip() == "":
        query = "*"
        highlight = "off"
    solr = pysolr.Solr(solr_url)
    results = solr.search(
        query,
        **{
            "df": "context",
            "fq": filter_query,
            "hl": highlight,
            "hl.fl": "context",
            "hl.fragsize": 0,
            "hl.snippets": 2,
            "group": "on",
            "group.field": "domain",
            "group.limit": 10,
            "rows": rows,
        },
    )
    return results
