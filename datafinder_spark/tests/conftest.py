import os
import shutil

import pytest

pytest.importorskip("pyspark")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """A Spark session of its own, in this process (``local``), with the settings that make its results those of the other engines: timestamps
    without a time zone, as a TIMESTAMP is in DuckDB, and UTC for the ones that have one."""
    if shutil.which("java") is None and not os.environ.get("JAVA_HOME"):
        pytest.skip("Spark needs Java, and there is none")
    from pyspark.sql import SparkSession
    session = (SparkSession.Builder().master("local[2]").appName("datafinder-specs")
               .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
               .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
               .config("spark.sql.session.timeZone", "UTC").config("spark.sql.timestampType", "TIMESTAMP_NTZ")
               .getOrCreate())
    session.sparkContext.setLogLevel("OFF")
    yield session
    session.stop()
