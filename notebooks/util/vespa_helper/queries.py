import re

from sentence_transformers import SentenceTransformer
from vespa.application import Vespa

model = SentenceTransformer('distiluse-base-multilingual-cased')


def get_facets(vespa_url: str, facet_field: str):
    app = Vespa(url=vespa_url)
    response = app.query(
        body={
            "yql": f"select * from sources * where active = true | all(group({facet_field}) each(output(count())) );",
            "hits": 0,
        }
    )
    groups = response.hits[0].get("children", [])
    for group in groups:
        if group.get("label") == "src_annotations":
            annotation_groups = group.get("children", [])
            facets = [a.get("value") for a in annotation_groups]
            return facets


def format_filter_query(filters: [str]):
    filter_query = ""
    if len(filters) > 0:
        filter_query = "and src_annotations contains "
    for f in filters:
        filter_query += f'"{f}" or src_annotations contains '
    filter_query = re.sub(" or src_annotations contains $", "", filter_query)
    return filter_query


def get_links(
    vespa_url: str, query: str, filters: [str], rows: int
):
    filter_query = format_filter_query(filters)
    app = Vespa(url=vespa_url)
    if query.strip() == "":
        links = app.query(
            body={
                "yql": f"select * from sources * where active = true {filter_query} | all(group(domain) max({rows}) each(max(10) each(output(summary()))));"
            }
        )
    else:
        embedding = model.encode([query])[0]
        links = app.query(
            body={
                "yql": f'select * from sources * where ([{{"targetHits": 10}}]nearestNeighbor(context_bert, tensor_bert)) and active = true {filter_query} | all(group(domain) max({rows}) each(max(10) each(output(summary()))));',
                "ranking.features.query(tensor_bert)": embedding.tolist(),
                "ranking.profile": "bert_context",
            }
        )
    return links
