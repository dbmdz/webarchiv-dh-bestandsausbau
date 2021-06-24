from chispa import assert_df_equality

from links_in_context.filter_records import (
    exclude_dest_hosts,
    filter_links_in_context,
    merge_links_in_context,
    linkcontext_schema,
)


def test_exclude_domains(spark):
    source_data = [("abc.de",), ("def.uk",), ("ghi.fr",)]
    source_df = spark.createDataFrame(source_data, ["dest_host"])
    exclude_list = "test/resources/exclude.csv"
    actual_df = exclude_dest_hosts(source_df, exclude_list)
    expected_data = [("def.uk",)]
    expected_df = spark.createDataFrame(expected_data, ["dest_host"])
    assert_df_equality(actual_df, expected_df)


def test_filter_linkcontext_identical_domains(spark):
    source_data = [
        (
            "20200406",
            "test.de",
            "test.de",
            "test.de",
            "test.de",
            "hier",
            "Infos gibt es hier",
            1.0,
        ),
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "da",
            "Infos gibt es da.",
            1.0,
        ),
    ]
    columns = [
        "crawl_date",
        "src",
        "src_host",
        "dest",
        "dest_host",
        "anchor",
        "context",
        "inv_path_depth",
    ]
    source_df = spark.createDataFrame(source_data, columns)
    expected_data = [
        (
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            ["da"],
            ["Infos gibt es da."],
            1.0,
        )
    ]
    expected_df = spark.createDataFrame(expected_data, linkcontext_schema)
    actual_df = filter_links_in_context(source_df)
    assert_df_equality(actual_df, expected_df)


def test_filter_linkcontext_mailto(spark):
    source_data = [
        (
            "20200406",
            "test.de",
            "test.de",
            "mailto:nicht-test.de",
            "nicht-test.de",
            "hier",
            "Infos gibt es hier",
            1.0,
        ),
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "da",
            "Infos gibt es da.",
            1.0,
        ),
    ]
    columns = [
        "crawl_date",
        "src",
        "src_host",
        "dest",
        "dest_host",
        "anchor",
        "context",
        "inv_path_depth",
    ]
    source_df = spark.createDataFrame(source_data, columns)
    expected_data = [
        (
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            ["da"],
            ["Infos gibt es da."],
            1.0,
        )
    ]
    expected_df = spark.createDataFrame(expected_data, linkcontext_schema)
    actual_df = filter_links_in_context(source_df)
    assert_df_equality(actual_df, expected_df)


def test_filter_linkcontext_missing(spark):
    source_data = [
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "",
            "hier",
            "Infos gibt es hier",
            1.0,
        ),
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            None,
            "Infos gibt es hier",
            1.0,
        ),
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "da",
            "Infos gibt es da.",
            1.0,
        ),
    ]
    columns = [
        "crawl_date",
        "src",
        "src_host",
        "dest",
        "dest_host",
        "anchor",
        "context",
        "inv_path_depth",
    ]
    source_df = spark.createDataFrame(source_data, columns)
    expected_data = [
        (
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            ["da"],
            ["Infos gibt es da."],
            1.0,
        )
    ]
    expected_df = spark.createDataFrame(expected_data, linkcontext_schema)
    actual_df = filter_links_in_context(source_df)
    assert_df_equality(actual_df, expected_df)


def test_filter_linkcontext_agg(spark):
    source_data = [
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "hier",
            "Infos gibt es hier",
            1.0,
        ),
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "da",
            "Infos gibt es da",
            1.0,
        ),
    ]
    columns = [
        "crawl_date",
        "src",
        "src_host",
        "dest",
        "dest_host",
        "anchor",
        "context",
        "inv_path_depth",
    ]
    source_df = spark.createDataFrame(source_data, columns)
    expected_data = [
        (
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            ["hier", "da"],
            ["Infos gibt es hier", "Infos gibt es da"],
            1.0,
        )
    ]
    expected_df = spark.createDataFrame(expected_data, linkcontext_schema)
    actual_df = filter_links_in_context(source_df)
    assert_df_equality(actual_df, expected_df)


def test_merge_linkcontext(spark):
    source_data_1 = [
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "hier",
            "Infos gibt es hier",
            1.0,
        ),
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "da",
            "Infos gibt es da",
            1.0,
        ),
    ]
    source_data_2 = [
        (
            "20200406",
            "test.de",
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            "hier",
            "Infos gibt es hier",
            1.0,
        )
    ]
    columns = [
        "crawl_date",
        "src",
        "src_host",
        "dest",
        "dest_host",
        "anchor",
        "context",
        "inv_path_depth",
    ]
    source_df_1 = spark.createDataFrame(source_data_1, columns)
    source_df_2 = spark.createDataFrame(source_data_2, columns)
    source_df_1 = filter_links_in_context(source_df_1)
    source_df_2 = filter_links_in_context(source_df_2)
    expected_data = [
        (
            "test.de",
            "nicht-test.de",
            "nicht-test.de",
            ["hier", "da"],
            ["Infos gibt es hier", "Infos gibt es da"],
            1.0,
        )
    ]
    expected_df = spark.createDataFrame(expected_data, linkcontext_schema)
    actual_df = merge_links_in_context(source_df_1.union(source_df_2))
    assert_df_equality(expected_df, actual_df)
