import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "UrbanMobilityAnalytics") -> SparkSession:
    """Cria Spark local e disponibiliza o driver JDBC do PostgreSQL."""

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark_tmp = Path(
        os.environ.get("SPARK_LOCAL_DIR", "/home/melo/spark-tmp")
    ).expanduser()
    spark_tmp.mkdir(parents=True, exist_ok=True)

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.5",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.local.dir", str(spark_tmp))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
