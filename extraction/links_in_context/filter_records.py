from aut.udfs import extract_domain
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import collect_list, mean, explode, when, col, arrays_zip
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    DoubleType,
)

from links_in_context.extract_linkcontext import (
    normalize_url_udf,
    get_inv_path_depth_udf,
    extract_links_in_context_udf,
)
from .manage_encoding import extract_encoding_udf, is_charset_supported_udf, decode_udf


def keep_valid_pages(records: DataFrame) -> DataFrame:
    return (
        records.filter(records.crawl_date.isNotNull())
        .filter(
            ~(records.url.rlike(".*robots\\.txt$"))
            & (
                records.mime_type_web_server.rlike("text/html")
                | records.mime_type_web_server.rlike("application/xhtml\\+xml")
                | records.url.rlike("(?i).*htm$")
                | records.url.rlike("(?i).*html$")
            )
        )
        .filter(records.http_status_code.rlike("200"))
    )


def decode_pages(valid_pages: DataFrame) -> DataFrame:
    df = (
        valid_pages.withColumn("extracted_encoding", extract_encoding_udf("content"))
        .withColumn(
            "supported_encoding",
            when(
                is_charset_supported_udf("extracted_encoding"),
                col("extracted_encoding"),
            ).otherwise("UTF-8"),
        )
        .select(
            "crawl_date",
            "url",
            decode_udf("bytes", "supported_encoding").alias("content"),
        )
    )
    return df


def get_links_in_context(decoded_pages: DataFrame) -> DataFrame:
    df = decoded_pages.withColumn(
        "link_extracts", extract_links_in_context_udf("content")
    ).withColumn("link_extracts", explode("link_extracts"))
    df = (
        df.withColumn("dest", df.link_extracts[0])
        .withColumn("anchor", df.link_extracts[1])
        .withColumn("context", df.link_extracts[2])
        .select(
            "crawl_date",
            normalize_url_udf("url").alias("src"),
            normalize_url_udf(extract_domain("url")).alias("src_host"),
            normalize_url_udf("dest").alias("dest"),
            normalize_url_udf(extract_domain("dest")).alias("dest_host"),
            "anchor",
            "context",
        )
        .withColumn("inv_path_depth", get_inv_path_depth_udf("dest"))
    )
    return df


def filter_links_in_context(df: DataFrame) -> DataFrame:
    df = (
        df.filter(
            df.src.isNotNull()
            & df.src_host.isNotNull()
            & df.dest.isNotNull()
            & df.dest_host.isNotNull()
            & df.anchor.isNotNull()
            & df.context.isNotNull()
        )
        .filter(
            ~(
                df.src.like("")
                | df.src_host.like("")
                | df.dest.like("")
                | df.dest_host.like("")
                | df.anchor.like("")
                | df.context.like("")
            )
        )
        .filter(df.src_host != df.dest_host)
        .filter(~df.dest.rlike("^mailto:.*"))
        .drop_duplicates(["dest", "anchor", "context"])
        .groupBy(["src_host", "dest", "dest_host"])
        .agg(
            collect_list("anchor").alias("anchor"),
            collect_list("context").alias("context"),
            mean("inv_path_depth").alias("inv_path_depth"),
        )
        .select("src_host", "dest", "dest_host", "anchor", "context", "inv_path_depth")
    )
    return df


def exclude_dest_hosts(df: DataFrame, exclude: str) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    exclude_df = spark.read.csv(exclude).toDF("dest_host")
    return df.join(exclude_df, on=["dest_host"], how="left_anti")


linkcontext_schema = StructType(
    [
        StructField("src_host", StringType(), True),
        StructField("dest", StringType(), True),
        StructField("dest_host", StringType(), True),
        StructField("anchor", ArrayType(StringType(), False), False),
        StructField("context", ArrayType(StringType(), False), False),
        StructField("inv_path_depth", DoubleType(), True),
    ]
)


def merge_links_in_context(to_merge: DataFrame) -> DataFrame:
    return (
        to_merge.withColumn("zipped", arrays_zip("anchor", "context"))
        .withColumn("zipped", explode("zipped"))
        .select(
            "src_host",
            "dest",
            "dest_host",
            col("zipped.anchor").alias("anchor"),
            col("zipped.context").alias("context"),
            "inv_path_depth",
        )
        .drop_duplicates()
        .groupBy(["src_host", "dest", "dest_host"])
        .agg(
            collect_list("anchor").alias("anchor"),
            collect_list("context").alias("context"),
            mean("inv_path_depth").alias("inv_path_depth"),
        )
        .select("src_host", "dest", "dest_host", "anchor", "context", "inv_path_depth")
    )
