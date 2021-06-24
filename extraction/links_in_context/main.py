from pathlib import Path
from shutil import rmtree

from aut import WebArchive
from pyspark import SparkContext
from pyspark.sql import SparkSession

from links_in_context.filter_records import (
    decode_pages,
    get_links_in_context,
    filter_links_in_context,
    exclude_dest_hosts,
    merge_links_in_context,
    linkcontext_schema,
    keep_valid_pages
)

sc = SparkContext.getOrCreate()
sqlContext = SparkSession.builder.getOrCreate()
spark = SparkSession.builder.appName("ExtractLinkcontext").getOrCreate()

input_path = Path("/in")
output_path = Path("/out")

seed_list = "links_in_context/Collection_Seeds.csv"
exclude_list = "links_in_context/Exclude_Domains.csv"


for file in input_path.iterdir():
    if file.is_dir():
        warc_pattern = "*.warc.gz"
        warc_path = file / "arcs" / warc_pattern
        extract_path = output_path / file.name

        if Path(extract_path, "_SUCCESS").exists():
            continue
        else:
            if extract_path.exists():
                rmtree(str(extract_path))

        target_instance_size = sum(
            warc.stat().st_size for warc in Path(file / "arcs").glob(warc_pattern)
        )

        if target_instance_size < 6000000000:
            records = WebArchive(sc, sqlContext, str(warc_path)).all()
            valid_pages = keep_valid_pages(records)
            decoded_pages = decode_pages(valid_pages)
            links_in_context = get_links_in_context(decoded_pages)
            links_in_context = filter_links_in_context(links_in_context)
            links_in_context = exclude_dest_hosts(links_in_context, seed_list)
            links_in_context = exclude_dest_hosts(links_in_context, exclude_list)
            links_in_context.coalesce(1).write.format("json").save(str(extract_path))

        else:
            suffix = "_linkcontext"
            for warc_path in Path(file / "arcs").iterdir():
                tmp_output_path = output_path / (file.name + "_tmp")
                extract_path = tmp_output_path / (warc_path.stem + suffix)
                records = WebArchive(sc, sqlContext, str(warc_path)).all()
                valid_pages = keep_valid_pages(records)
                decoded_pages = decode_pages(valid_pages)
                links_in_context = get_links_in_context(decoded_pages)
                links_in_context = filter_links_in_context(links_in_context)
                links_in_context = exclude_dest_hosts(links_in_context, seed_list)
                links_in_context = exclude_dest_hosts(links_in_context, exclude_list)
                links_in_context.coalesce(1).write.format("json").save(
                    str(extract_path)
                )

            extracts_path = Path(tmp_output_path / ("*" + suffix))
            merge_path = Path(output_path / file.name)
            to_merge = (
                spark.read.format("json")
                .schema(linkcontext_schema)
                .option("path", str(extracts_path))
                .load()
            )
            merged = merge_links_in_context(to_merge)
            merged.coalesce(1).write.format("json").save(str(merge_path))
            rmtree(str(tmp_output_path))

extracts_path = Path(output_path / "*" / "part-00000-*.json")
merge_path = Path(output_path / "all_links_in_context")
to_merge = (
    spark.read.format("json")
    .schema(linkcontext_schema)
    .option("path", str(extracts_path))
    .load()
)
merged = merge_links_in_context(to_merge)
merged.coalesce(1).write.format("json").save(str(merge_path))
