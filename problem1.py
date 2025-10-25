import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand

def main(master_url, net_id):
    # 1) Initialize Spark (S3 support via hadoop-aws + AWS SDK bundle)
    spark = (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        .master(master_url)
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .getOrCreate()
    )

    # 2) Read raw logs from S3 (each application dir contains .log files)
    logs_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/*/*.log"
    logs_df = spark.read.text(logs_path)

    total_lines = logs_df.count()
    print(f"Total log lines read: {total_lines}")

    # 3) Parse log level (INFO/WARN/ERROR/DEBUG); cache because we reuse it
    logs_parsed = logs_df.select(
        col("value").alias("log_entry"),
        regexp_extract("value", r"(INFO|WARN|ERROR|DEBUG)", 1).alias("log_level")
    ).cache()

    total_with_levels = logs_parsed.filter(col("log_level") != "").count()
    print(f"Lines with log levels: {total_with_levels}")

    # 4) Aggregate counts by log level (exclude empty matches)
    counts_df = (
        logs_parsed.filter(col("log_level") != "")
        .groupBy("log_level")
        .count()
        .orderBy("count", ascending=False)
        .cache()
    )
    print("Counts preview:")
    counts_df.show(10, truncate=False)

    # 5) Random sample (10 rows with non-empty level)
    sample_df = (
        logs_parsed.filter(col("log_level") != "")
        .orderBy(rand())
        .limit(10)
        .cache()
    )
    print("Sample preview:")
    sample_df.show(10, truncate=False)

    # 6) WRITE OUTPUTS TO S3 (shared filesystem) — THIS FIXES THE _SUCCESS-ONLY ISSUE
    s3_base = f"s3a://{net_id}-assignment-spark-cluster-logs/output"
    counts_output = f"{s3_base}/problem1_counts"
    sample_output = f"{s3_base}/problem1_sample"

    # Overwrite S3 folders cleanly; Spark will create part-*.csv there
    counts_df.coalesce(1).write.mode("overwrite").option("header", True).csv(counts_output)
    sample_df.coalesce(1).write.mode("overwrite").option("header", True).csv(sample_output)

    # 7) Write summary locally on master for easy scp
    # (CSV are on S3; the text summary is a single small file, fine to keep local)
    summary_lines = [
        f"Total log lines processed: {total_lines}",
        f"Total lines with log levels: {total_with_levels}",
        f"Lines without log levels: {total_lines - total_with_levels}",
        "",
        "Log level distribution:"
    ]
    counts_local = counts_df.collect()
    total_levels = sum([r["count"] for r in counts_local])
    for r in counts_local:
        lvl = r["log_level"]
        cnt = r["count"]
        pct = (cnt / total_levels * 100.0) if total_levels > 0 else 0.0
        summary_lines.append(f"  {lvl:<6}: {cnt:>10} ({pct:6.2f}%)")

    summary_path_local = "/home/ubuntu/spark-cluster/data/output/problem1_summary.txt"
    with open(summary_path_local, "w") as f:
        f.write("\n".join(summary_lines))
    print(f"Summary written to {summary_path_local}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("master_url", type=str, help="Spark master URL (e.g., spark://...:7077)")
    parser.add_argument("--net-id", type=str, required=True, help="Your net ID for S3 bucket")
    args = parser.parse_args()
    main(args.master_url, args.net_id)

