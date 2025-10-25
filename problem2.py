import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract,
    col,
    min as spark_min,
    max as spark_max,
    try_to_timestamp,
    lit,
    countDistinct,
)
import pandas as pd

# Try seaborn first per assignment; fallback to matplotlib if missing
_USE_SEABORN = True
try:
    import seaborn as sns  # noqa: F401
    import matplotlib.pyplot as plt
except Exception:
    _USE_SEABORN = False
    import matplotlib.pyplot as plt  # fallback without seaborn


def _ensure_dirs(path: str):
    os.makedirs(path, exist_ok=True)


def main(master_url: str, net_id: str, skip_spark: bool = False):
    """
    Problem 2: Cluster Usage Analysis
    - Build application timeline per (cluster_id, application_id)
    - Build cluster summary with counts and first/last app timestamps
    - Write:
        data/output/problem2_timeline.csv
        data/output/problem2_cluster_summary.csv
        data/output/problem2_stats.txt
        data/output/problem2_bar_chart.png
        data/output/problem2_density_plot.png
    """

    # ---------- Paths ----------
    base_output = "/home/ubuntu/spark-cluster/data/output"
    _ensure_dirs(base_output)

    timeline_csv = os.path.join(base_output, "problem2_timeline.csv")
    cluster_summary_csv = os.path.join(base_output, "problem2_cluster_summary.csv")
    stats_txt = os.path.join(base_output, "problem2_stats.txt")
    bar_png = os.path.join(base_output, "problem2_bar_chart.png")
    density_png = os.path.join(base_output, "problem2_density_plot.png")

    if skip_spark:
        # Re-generate plots/stats from existing CSVs
        apps_df = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
        clusters_df = pd.read_csv(cluster_summary_csv, parse_dates=["cluster_first_app", "cluster_last_app"])
    else:
        # ---------- Spark Session ----------
        spark = (
            SparkSession.builder.appName("Problem2_ClusterUsageAnalysis")
            .master(master_url)
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider",
            )
            .getOrCreate()
        )

        # ---------- Load & Parse ----------
        # Use broad glob like Problem 1; do NOT restrict to *.log to avoid missing files
        logs_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/*/*"
        logs_df = spark.read.text(logs_path)

        # Extract raw fields from each log line
        parsed = logs_df.select(
            # raw timestamp string
            regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("timestamp"),
            # full application_id, e.g. application_1485248649253_0052
            regexp_extract(col("value"), r"(application_\d+_\d+)", 1).alias("application_id"),
            # cluster_id: the middle large number
            regexp_extract(col("value"), r"application_(\d+)_\d+", 1).alias("cluster_id"),
        )

        # Convert timestamp with tolerance; filter null/empty app ids
        parsed = parsed.withColumn(
            "ts", try_to_timestamp(col("timestamp"), lit("yy/MM/dd HH:mm:ss"))
        ).filter(col("ts").isNotNull() & (col("application_id") != ""))

        # ---------- Application-level timeline ----------
        # For each (cluster_id, application_id), take min/max timestamp
        apps_df_spark = parsed.groupBy("cluster_id", "application_id").agg(
            spark_min("ts").alias("start_time"),
            spark_max("ts").alias("end_time"),
        )

        # Add app_number (e.g., 0052)
        apps_df_spark = apps_df_spark.withColumn(
            "app_number", regexp_extract(col("application_id"), r"application_\d+_(\d+)", 1)
        )

        # Bring to pandas and write single CSV
        apps_df = apps_df_spark.toPandas()
        # Ensure ordering roughly by cluster then app_number numeric order when possible
        # app_number may have leading zeros; keep original string but sort by int copy
        try:
            apps_df["_app_num_int"] = apps_df["app_number"].astype(int)
            apps_df = apps_df.sort_values(["cluster_id", "_app_num_int"])
            apps_df = apps_df.drop(columns=["_app_num_int"])
        except Exception:
            apps_df = apps_df.sort_values(["cluster_id", "app_number"])

        # Format datetimes as strings for CSV
        apps_df["start_time"] = pd.to_datetime(apps_df["start_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        apps_df["end_time"] = pd.to_datetime(apps_df["end_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

        apps_df[["cluster_id", "application_id", "app_number", "start_time", "end_time"]].to_csv(
            timeline_csv, index=False
        )

        # ---------- Cluster-level summary ----------
        clusters_df_spark = apps_df_spark.groupBy("cluster_id").agg(
            countDistinct("application_id").alias("num_applications"),
            spark_min("start_time").alias("cluster_first_app"),
            spark_max("end_time").alias("cluster_last_app"),
        )

        clusters_df = clusters_df_spark.toPandas()
        clusters_df = clusters_df.sort_values("cluster_id")
        clusters_df["cluster_first_app"] = pd.to_datetime(clusters_df["cluster_first_app"]).dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        clusters_df["cluster_last_app"] = pd.to_datetime(clusters_df["cluster_last_app"]).dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        clusters_df[["cluster_id", "num_applications", "cluster_first_app", "cluster_last_app"]].to_csv(
            cluster_summary_csv, index=False
        )

        spark.stop()

    # ---------- Stats ----------
    total_clusters = clusters_df["cluster_id"].nunique()
    total_apps = apps_df["application_id"].nunique()
    avg_apps_per_cluster = (total_apps / total_clusters) if total_clusters else 0.0

    top_clusters = clusters_df.sort_values("num_applications", ascending=False).head(5)

    with open(stats_txt, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for _, row in top_clusters.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications\n")

    # ---------- Visualizations ----------
    # Bar Chart: applications per cluster (with value labels)
    plt.figure(figsize=(10, 6))
    clusters_sorted = clusters_df.sort_values("num_applications", ascending=False)
    x = clusters_sorted["cluster_id"].astype(str)
    y = clusters_sorted["num_applications"].astype(int)

    if _USE_SEABORN:
        import seaborn as sns  # guaranteed available here
        ax = sns.barplot(x=x, y=y)
    else:
        ax = plt.bar(x, y)

    plt.title("Applications per Cluster")
    plt.ylabel("Number of Applications")
    plt.xlabel("Cluster ID")
    plt.xticks(rotation=45, ha="right")
    # value labels
    if _USE_SEABORN:
        for i, v in enumerate(y):
            plt.text(i, v, str(v), ha="center", va="bottom", fontsize=9)
    else:
        for xi, yi in zip(plt.gca().get_xticks(), y):
            plt.text(xi, yi, str(yi), ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    plt.savefig(bar_png)
    plt.close()

    # Density Plot (largest cluster): histogram + KDE, log-scale x-axis, title with n
    # Compute durations per application (minutes)
    apps_df_local = apps_df.copy()
    apps_df_local["start_time"] = pd.to_datetime(apps_df_local["start_time"])
    apps_df_local["end_time"] = pd.to_datetime(apps_df_local["end_time"])
    apps_df_local["duration_min"] = (apps_df_local["end_time"] - apps_df_local["start_time"]).dt.total_seconds() / 60.0

    # largest cluster by number of applications
    largest_cluster = (
        apps_df_local.groupby("cluster_id")["application_id"].nunique().sort_values(ascending=False).index[0]
    )
    largest_apps = apps_df_local[apps_df_local["cluster_id"] == largest_cluster].copy()
    # Remove non-positive/NaN durations to avoid log-scale issues
    largest_apps = largest_apps[largest_apps["duration_min"] > 0].dropna(subset=["duration_min"])
    n_samples = len(largest_apps)

    plt.figure(figsize=(10, 6))
    if _USE_SEABORN:
        import seaborn as sns
        sns.histplot(largest_apps["duration_min"], bins=50, stat="density", alpha=0.6)
        sns.kdeplot(largest_apps["duration_min"])
    else:
        # basic matplotlib fallback
        plt.hist(largest_apps["duration_min"], bins=50, density=True, alpha=0.6)

    plt.xscale("log")
    plt.title(f"Job Duration Distribution for Cluster {largest_cluster} (n={n_samples})")
    plt.xlabel("Duration (minutes, log scale)")
    plt.ylabel("Density")
    plt.tight_layout()
    plt.savefig(density_png)
    plt.close()

    print(f"Wrote: {timeline_csv}")
    print(f"Wrote: {cluster_summary_csv}")
    print(f"Wrote: {stats_txt}")
    print(f"Wrote: {bar_png}")
    print(f"Wrote: {density_png}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("master_url", type=str, nargs="?", help="Spark master URL (e.g., spark://...:7077)")
    parser.add_argument("--net-id", type=str, help="Your net ID for S3 bucket")
    parser.add_argument("--skip-spark", action="store_true", help="Skip Spark and regenerate visuals from CSVs")
    args = parser.parse_args()

    main(args.master_url, args.net_id, skip_spark=args.skip_spark)


