import codecs
from typing import Dict

from bs4 import BeautifulSoup, SoupStrainer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType


def parse_headers(response: str) -> Dict:
    lines = response.split("\n")
    headers = {}
    for line in lines[1:]:
        try:
            header_key, header_value = line.split(":", maxsplit=1)
            elems = {}
            for elem in header_value.split(";"):
                if len(elem.split("=")) < 2:
                    elems[elem.strip()] = ""
                else:
                    elem_key, elem_value = elem.split("=", maxsplit=1)
                    elems[elem_key.strip()] = elem_value.strip()
            headers[header_key.strip()] = elems
        except ValueError:
            continue
    return headers


def split_headers_from_content(response: str) -> (str, str):
    try:
        headers, html = response.split("\r\n", maxsplit=1)
        return headers, html
    except ValueError:
        return None, None


def extract_encoding_from_headers(headers: str) -> str:
    parsed_headers = parse_headers(headers)
    if "Content-Type" in parsed_headers.keys():
        if "charset" in parsed_headers["Content-Type"].keys():
            return str(parsed_headers["Content-Type"]["charset"])
    else:
        return None


def extract_encoding_from_meta_tags(html: str) -> str:
    meta_tags = SoupStrainer("meta")
    parsed_html = BeautifulSoup(html, features="html.parser", parse_only=meta_tags)
    meta_tag = parsed_html.find("meta", attrs={"http-equiv": "Content-Type"})
    if meta_tag is not None:
        try:
            mime_type, charset = meta_tag["content"].split(";", maxsplit=1)
            return str(charset.split("=", maxsplit=1)[1])
        except ValueError:
            pass
    meta_tag = parsed_html.find("meta", attrs={"charset": True})
    if meta_tag is not None:
        return str(meta_tag["charset"])
    return None


def extract_encoding(response: str) -> str:
    """Extract information about text encoding from HTTP Header or HTML meta tags.

    Looks for charset in HTTP Content-Type Header and for HTML meta tags with attributes
    http-equiv="Content-Type" and content="text/html; charset=XXX" or attribute charset="XXX".

    response -- http response

    :return:
    """
    default_charset = "UTF-8"
    headers, html = split_headers_from_content(response)
    if headers is None and html is None:
        return default_charset
    charset = extract_encoding_from_headers(headers)
    if charset is not None:
        return charset
    charset = extract_encoding_from_meta_tags(html)
    if charset is not None:
        return charset
    return default_charset


def decode(content_bytes: bytes, charset: str) -> str:
    return content_bytes.decode(charset, errors="replace")


def is_charset_supported(charset: str):
    try:
        codecs.lookup(charset)
    except LookupError:
        return False
    return True


is_charset_supported_udf = udf(is_charset_supported, BooleanType())
extract_encoding_udf = udf(extract_encoding, StringType())
decode_udf = udf(decode, StringType())
