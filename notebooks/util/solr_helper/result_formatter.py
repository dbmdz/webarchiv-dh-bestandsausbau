def style_highlight(highlight):
    style = '<em style="background-color:yellow;font-style:normal;">'
    styled_highlight = highlight.replace("<em>", style)
    return styled_highlight


def write_table_row(key, value):
    row = f"<tr><td>{key}</td><td>{value}</td></tr>"
    return row


def write_doc_entry(idx, doc, doc_highlight):
    doc_entry = f'<tr><th colspan="2">Link {idx}</th></tr>'
    doc_entry += write_table_row(
        "URL", f'<a target="_blank" rel="noopener noreferrer" href="//www.{doc["url"]}">{doc["url"]}</a>',
    )
    joined_contexts = "<br>".join(doc_highlight["context"][:3])
    doc_entry += write_table_row("Kontext", style_highlight(joined_contexts))
    doc_entry += write_table_row("", "")
    return doc_entry


def write_domain_table(domain, highlights):
    domain_table = "<table><style>th, td { padding-left: 15px; text-align: left; vertical-align: top;}</style>"
    for i, doc in enumerate(domain["doclist"]["docs"]):
        if highlights:
            doc_highlight = highlights[doc["url"]]
        else:
            doc_highlight = doc
        domain_table += write_doc_entry(i + 1, doc, doc_highlight)
    domain_table += "</table>"
    return domain_table
