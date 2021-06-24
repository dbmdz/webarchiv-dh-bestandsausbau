def write_table_row(key, value):
    row = f"<tr><td>{key}</td><td>{value}</td></tr>"
    return row


def write_doc_entry(idx, domain):
    fields = domain.get("fields")
    doc_entry = f'<tr><th colspan="2">Link {idx}</th></tr>'
    doc_entry += write_table_row(
        "URL",
        f"<a target=\"_blank\" rel=\"noopener noreferrer\" href=\"//www.{fields.get('url')}\">{fields.get('url')}</a>",
    )
    doc_entry += write_table_row("Kontext", fields.get("context"))
    doc_entry += write_table_row("", "")
    return doc_entry


def write_domain_table(domain_group):
    domain_table = "<table><style>th, td { padding-left: 15px; text-align: left; vertical-align: top;}</style>"
    for i, domain in enumerate(domain_group.get("children", [])):
        domain_table += write_doc_entry(i + 1, domain)
    domain_table += "</table>"
    return domain_table
