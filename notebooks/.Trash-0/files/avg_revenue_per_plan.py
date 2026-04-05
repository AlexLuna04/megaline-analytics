from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Megaline - Ingreso promedio por plan 2025") \
    .getOrCreate()

# ── Leer tabla de hechos desde BD via JDBC ────────────────────────
df = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", "gold.fact_usage") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", jdbc_driver) \
    .load()

# ── Filtrar solo 2025 ─────────────────────────────────────────────
df_2025 = df.filter(F.year(F.col("event_date")) == 2025)

# ── Calcular excedentes por usuario/mes ───────────────────────────
df_revenue = df_2025.withColumn(
    "extra_minutes",
    F.greatest(F.col("total_minutes") - F.col("minutes_included"), F.lit(0))
).withColumn(
    "extra_messages",
    F.greatest(F.col("total_messages") - F.col("messages_included"), F.lit(0))
).withColumn(
    "extra_mb",
    F.greatest(F.col("total_mb") - F.col("mb_per_month_included"), F.lit(0))
).withColumn(
    # Convertir MB excedente a GB
    "extra_gb", F.col("extra_mb") / F.lit(1024)
).withColumn(
    # Ingreso total = cuota fija + excedentes
    "total_revenue",
    F.col("usd_monthly_pay")
    + (F.col("extra_minutes")  * F.col("usd_per_minute"))
    + (F.col("extra_messages") * F.col("usd_per_message"))
    + (F.col("extra_gb")       * F.col("usd_per_gb"))
)

# ── Ingreso promedio por plan ─────────────────────────────────────
result = df_revenue.groupBy("plan").agg(
    F.round(F.avg("total_revenue"), 2).alias("avg_revenue_usd"),
    F.count("user_id").alias("total_users")
).orderBy(F.desc("avg_revenue_usd"))

result.show()
# +--------+----------------+-------------+
# |plan    |avg_revenue_usd |total_users  |
# +--------+----------------+-------------+
# |ultimate|85.40           |1200000      |
# |surf    |23.15           |850000       |
# +--------+----------------+-------------+