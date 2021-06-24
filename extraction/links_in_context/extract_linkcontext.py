import re
from typing import List

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, FloatType


def normalize_whitespace(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\s+", " ", text)
    return text


def extract_links_in_context(html: str) -> List[List[str]]:
    parsed_html = BeautifulSoup(html, features="html.parser")
    links = parsed_html.find_all("a", attrs={"href": True})
    results = []
    for link in links:
        dest = link.attrs["href"]
        anchor = link.text
        siblings = link.find_next_siblings(
            "a", attrs={"href": True}
        ) + link.find_previous_siblings("a", attrs={"href": True})
        if len(siblings) > 1:
            context = link.text
        else:
            context = link.parent.text
            if len(context) > 1000:
                anchor_index = context.index(link.text)
                start_index = max(0, anchor_index - 500)
                end_index = min(len(context) - 1, anchor_index + 500)
                context = context[start_index:end_index]
        anchor = normalize_whitespace(anchor)
        context = normalize_whitespace(context)
        results.append((dest, anchor, context))
    return results


extract_links_in_context_udf = udf(
    extract_links_in_context, ArrayType(ArrayType(StringType()))
)


def normalize_url(url: str) -> str:
    url = url.strip()
    url = url.lower()
    url = re.sub(r"^(https?://)?(www\.)?", "", url)
    url = re.sub(r"/$", "", url)
    return url


normalize_url_udf = udf(normalize_url, StringType())


def get_inv_path_depth(url: str) -> float:
    path_depth = url.count("/") + 1
    return float(1 / path_depth)


get_inv_path_depth_udf = udf(get_inv_path_depth, FloatType())
