from tecton import batch_feature_view, FilteredSource, Aggregation, DatabricksClusterConfig
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta
from tecton.types import Field, String, Int32, Timestamp, Int64

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='pyspark',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    batch_compute = DatabricksClusterConfig(
                instance_type = 'm5.2xlarge',
                spark_config = {"spark.executor.memory" : "12g"},
                extra_pip_dependencies=["fuzzywuzzy"],
    ),
    schema = [
       Field("user_id", String),
       Field("transaction", Int32),
       Field("timestamp", Timestamp),
    ],
    run_query_validation = False,
    batch_schedule = timedelta(days=1),
    description='User transaction totals over a series of time windows, updated daily.'
)
def user_transaction_counts_python_dependencies(transactions):
    import fuzzywuzzy
    from fuzzywuzzy import fuzz
    from pyspark.sql import functions as f
    from pyspark.sql.types import StringType

    # Apply Fuzzy Function
    def matchstring(s1, s2):
       return fuzz.token_sort_ratio(s1, s2)

    MatchUDF = f.udf(matchstring, StringType())
    return transactions.select("user_id", MatchUDF(f.col("user_id"), f.col("user_id")).cast("int").alias("transaction"), "timestamp")
