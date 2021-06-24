from chispa import assert_df_equality
from pyspark.sql.functions import explode

from links_in_context.extract_linkcontext import (
    extract_links_in_context_udf,
    normalize_url_udf,
    get_inv_path_depth_udf,
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def test_extract_linkcontext(spark):
    source_data = [
        (
            "20200406",
            "http://www.test.com",
            r'<!DOCTYPE html>\
                    <html lang="en">\
                    <head>\
                    <meta charset="utf-8">\
                    <title>title</title>\
                    <link rel="stylesheet" href="style.css">\
                    <script src="script.js"></script>\
                    </head>\
                    <body>\
                    <p>Weitere Infos finden Sie <a href="http://www.xy.de">hier</a> und <a href="http://www.xyz.de">da</a>.</p>\
                    </body>\
                    </html>',
        )
    ]
    source_df = spark.createDataFrame(source_data, ["crawl_date", "src", "content"])
    source_df = source_df.select(
        "crawl_date",
        "src",
        extract_links_in_context_udf("content").alias("link_extracts"),
    )
    source_df = source_df.withColumn("link_extracts", explode(source_df.link_extracts))
    actual_df = (
        source_df.withColumn("dest", source_df.link_extracts[0])
        .withColumn("anchor", source_df.link_extracts[1])
        .withColumn("context", source_df.link_extracts[2])
        .select("crawl_date", "src", "dest", "anchor", "context")
    )
    expected_data = [
        (
            "20200406",
            "http://www.test.com",
            "http://www.xy.de",
            "hier",
            "Weitere Infos finden Sie hier und da.",
        ),
        (
            "20200406",
            "http://www.test.com",
            "http://www.xyz.de",
            "da",
            "Weitere Infos finden Sie hier und da.",
        ),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["crawl_date", "src", "dest", "anchor", "context"]
    )
    assert_df_equality(actual_df, expected_df)


def test_extract_linkcontext_linklist(spark):
    source_data = [
        (
            "20200406",
            "http://www.test.com",
            r'<!DOCTYPE html>\
                    <html lang="en">\
                    <head>\
                    <meta charset="utf-8">\
                    <title>title</title>\
                    <link rel="stylesheet" href="style.css">\
                    <script src="script.js"></script>\
                    </head>\
                    <body>\
                    <p>Weitere Infos finden Sie <a href="http://www.xy.de">hier</a> und <a href="http://www.xyz.de">da</a> und <a href="http://www.xyz.de">dort</a>.</p>\
                    </body>\
                    </html>',
        )
    ]
    source_df = spark.createDataFrame(source_data, ["crawl_date", "src", "content"])
    source_df = source_df.select(
        "crawl_date",
        "src",
        extract_links_in_context_udf("content").alias("link_extracts"),
    )
    source_df = source_df.withColumn("link_extracts", explode(source_df.link_extracts))
    actual_df = (
        source_df.withColumn("dest", source_df.link_extracts[0])
        .withColumn("anchor", source_df.link_extracts[1])
        .withColumn("context", source_df.link_extracts[2])
        .select("crawl_date", "src", "dest", "anchor", "context")
    )

    actual_df.show()
    expected_data = [
        ("20200406", "http://www.test.com", "http://www.xy.de", "hier", "hier"),
        ("20200406", "http://www.test.com", "http://www.xyz.de", "da", "da"),
        ("20200406", "http://www.test.com", "http://www.xyz.de", "dort", "dort"),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["crawl_date", "src", "dest", "anchor", "context"]
    )
    assert_df_equality(actual_df, expected_df)


def test_extract_linkcontext_char_limit(spark):
    source_data = [
        (
            "20200406",
            "http://www.test.com",
            r'<!DOCTYPE html>\
                    <html lang="en">\
                    <head>\
                    <meta charset="utf-8">\
                    <title>title</title>\
                    <link rel="stylesheet" href="style.css">\
                    <script src="script.js"></script>\
                    </head>\
                    <body>\
                    <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc sit amet magna ac diam efficitur aliquet vitae ac neque. Sed vel vestibulum magna. Integer imperdiet nulla in est viverra porta. Mauris imperdiet faucibus nulla laoreet lacinia. Sed diam sapien, molestie ac odio tempus, lacinia faucibus tellus. Fusce cursus velit sit amet augue molestie, nec fringilla ligula laoreet. Nam diam neque, laoreet vel neque eu, cursus consequat diam. Phasellus sodales enim justo. Integer mattis viverra massa, eget pulvinar enim hendrerit sit amet. Nunc dapibus lacus nec rutrum scelerisque. In ut ligula eu eros sodales tempus. Nunc vel tincidunt neque. Praesent in blandit diam. Aliquam eget enim commodo nunc fringilla congue. Etiam ex lectus, imperdiet non velit sed, congue auctor nulla. Ut bibendum libero non egestas lacinia. Mauris gravida eleifend nunc, vitae egestas nulla vulputate sit amet. In sollicitudin odio eget arcu pretium, non tincidunt purus tincidunt. Nam sit amet condimentum elit. <a href="http://www.xy.de">Quisque</a> bibendum venenatis lectus a fermentum. Fusce maximus euismod porttitor. Nullam et lectus vitae massa vehicula rutrum eget vitae erat. Quisque quis justo et est vehicula malesuada. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Mauris sit amet justo id purus sollicitudin convallis finibus in elit. Proin sed pellentesque ligula, eget rutrum purus. Interdum et malesuada fames ac ante ipsum primis in faucibus. Etiam viverra vitae velit in finibus. Nulla dignissim justo vitae egestas volutpat. Ut luctus porttitor ligula, at congue tellus. Sed vehicula arcu ac turpis euismod interdum. Curabitur faucibus lacus quis augue porta, eget pulvinar nisi ornare.</p>\
                    </body>\
                    </html>',
        )
    ]
    source_df = spark.createDataFrame(source_data, ["crawl_date", "src", "content"])
    source_df = source_df.select(
        "crawl_date",
        "src",
        extract_links_in_context_udf("content").alias("link_extracts"),
    )
    source_df = source_df.withColumn("link_extracts", explode(source_df.link_extracts))
    actual_df = (
        source_df.withColumn("dest", source_df.link_extracts[0])
        .withColumn("anchor", source_df.link_extracts[1])
        .withColumn("context", source_df.link_extracts[2])
        .select("crawl_date", "src", "dest", "anchor", "context")
    )

    actual_df.show()
    expected_data = [
        (
            "20200406",
            "http://www.test.com",
            "http://www.xy.de",
            "Quisque",
            "ssa, eget pulvinar enim hendrerit sit amet. Nunc dapibus lacus nec rutrum scelerisque. In ut ligula eu eros sodales tempus. Nunc vel tincidunt neque. Praesent in blandit diam. Aliquam eget enim commodo nunc fringilla congue. Etiam ex lectus, imperdiet non velit sed, congue auctor nulla. Ut bibendum libero non egestas lacinia. Mauris gravida eleifend nunc, vitae egestas nulla vulputate sit amet. In sollicitudin odio eget arcu pretium, non tincidunt purus tincidunt. Nam sit amet condimentum elit. Quisque bibendum venenatis lectus a fermentum. Fusce maximus euismod porttitor. Nullam et lectus vitae massa vehicula rutrum eget vitae erat. Quisque quis justo et est vehicula malesuada. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Mauris sit amet justo id purus sollicitudin convallis finibus in elit. Proin sed pellentesque ligula, eget rutrum purus. Interdum et malesuada fames ac ante ipsum primis in faucibus. Etiam viverra vitae velit in finibus. Null",
        )
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["crawl_date", "src", "dest", "anchor", "context"]
    )
    assert_df_equality(actual_df, expected_df)


def test_normalize_url(spark):
    source_data = [("http://www.xyZ.de/",), ("https://xyz.de",), ("www.xyz.de",)]
    source_df = spark.createDataFrame(source_data, ["url"])
    expected_data = [("xyz.de",), ("xyz.de",), ("xyz.de",)]
    expected_df = spark.createDataFrame(expected_data, ["url"])
    actual_df = source_df.select(normalize_url_udf("url").alias("url"))
    assert_df_equality(actual_df, expected_df)


def test_get_inv_path_depth(spark):
    source_data = [("abc.de/fgh/ijk/lm",), ("abc.de/fgh/ijk/lmn/opq",), ("abc.de",)]
    source_df = spark.createDataFrame(source_data, ["url"])
    expected_data = [
        ("abc.de/fgh/ijk/lm", float(1 / 4)),
        ("abc.de/fgh/ijk/lmn/opq", float(1 / 5)),
        ("abc.de", float(1.0)),
    ]
    schema = StructType(
        [
            StructField("url", StringType(), True),
            StructField("inv_path_depth", FloatType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, schema)
    actual_df = source_df.select(
        "url", get_inv_path_depth_udf("url").alias("inv_path_depth")
    )
    assert_df_equality(actual_df, expected_df)


def test_normalize_whitespace(spark):
    source_data = [
        (
            "20200406",
            "http://www.test.com",
            '<!DOCTYPE html>\
                    <html lang="en">\
                    <head>\
                    <meta charset="utf-8">\
                    <title>title</title>\
                    <link rel="stylesheet" href="style.css">\
                    <script src="script.js"></script>\
                    </head>\
                    <body>\
                    <p>\r\nWeitere Infos    finden Sie <a href="http://www.xy.de"> hier</a>\tund <a href="http://www.xyz.de">da</a>.</p>\
                    </body>\
                    </html>',
        )
    ]
    source_df = spark.createDataFrame(source_data, ["crawl_date", "src", "content"])
    source_df = source_df.select(
        "crawl_date",
        "src",
        extract_links_in_context_udf("content").alias("link_extracts"),
    )
    source_df = source_df.withColumn("link_extracts", explode(source_df.link_extracts))
    actual_df = (
        source_df.withColumn("dest", source_df.link_extracts[0])
        .withColumn("anchor", source_df.link_extracts[1])
        .withColumn("context", source_df.link_extracts[2])
        .select("crawl_date", "src", "dest", "anchor", "context")
    )

    actual_df.show()
    expected_data = [
        (
            "20200406",
            "http://www.test.com",
            "http://www.xy.de",
            "hier",
            "Weitere Infos finden Sie hier und da.",
        ),
        (
            "20200406",
            "http://www.test.com",
            "http://www.xyz.de",
            "da",
            "Weitere Infos finden Sie hier und da.",
        ),
    ]
    expected_df = spark.createDataFrame(
        expected_data, ["crawl_date", "src", "dest", "anchor", "context"]
    )
    assert_df_equality(actual_df, expected_df)
