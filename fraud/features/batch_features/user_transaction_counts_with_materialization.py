from tecton import batch_feature_view, FilteredSource, Aggregation, DatabricksClusterConfig
from fraud.entities import user
from fraud.data_sources.transactions import transactions_batch
from datetime import datetime, timedelta
from tecton.types import Field, String, Int32, Timestamp, Int64

@batch_feature_view(
    sources=[FilteredSource(transactions_batch)],
    entities=[user],
    mode='spark_sql',
    online=True,
    offline=True,
    feature_start_time=datetime(2022, 5, 1),
    batch_compute = DatabricksClusterConfig(
        instance_type = 'm5.2xlarge',
        spark_config = {"spark.executor.memory" : "12g"},
        extra_pip_dependencies=["fuzzywuzzy"],
    ),
    tags={'release': 'production'},
    owner='matt@tecton.ai',
    schema = [
       Field("user_id", String),
       Field("transaction", Int32),
       Field("timestamp", Timestamp),
    ],
    run_query_validation = False,
    batch_schedule = timedelta(days=1),
    description='User transaction totals over a series of time windows, updated daily.'
)
def user_transaction_counts_with_materialization(transactions):
    return f'''
        SELECT
            user_id,
            1 as transaction,
            timestamp
        FROM
            {transactions}
        '''
