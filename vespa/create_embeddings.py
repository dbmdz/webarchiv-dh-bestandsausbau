import click
import jsonlines

from sentence_transformers import SentenceTransformer

embedder = SentenceTransformer("distiluse-base-multilingual-cased")


@click.command()
@click.argument("src")
@click.argument("dest")
def create_embeddings(src, dest):
    with open(dest, "w") as embedding_file:
        with open(src, "r") as link_file:
            reader = jsonlines.Reader(link_file)
            writer = jsonlines.Writer(embedding_file)
            for link in reader:
                if link["dest"] == "" or link["dest_host"] == "":
                    continue
                contexts = link["context"]
                for context in contexts:
                    context = context.strip()
                    id = hash(link["dest"] + context)
                    embedding = embedder.encode([context])[0]
                    doc = dict()
                    doc["put"] = f"id:linkcontext:linkcontext::{id}"
                    doc["fields"] = {
                        "id": id,
                        "src_annotations": link["src_annotations"],
                        "url": link["dest"],
                        "domain": link["dest_host"],
                        "context": context,
                        "context_bert": {"values": embedding.tolist()},
                        "inv_path_depth": link["inv_path_depth"],
                        "active": True,
                    }
                    writer.write(doc)


if __name__ == "__main__":
    create_embeddings()
