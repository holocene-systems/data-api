from ...common.config import (
    TZ
)

# ------------------------------------------------------------------------------
# POSTGRES QUERY FUNCTION

def query_one_sensor_rollup_monthly(postgres_table_model, all_datetimes, sensor_id):
    """Builds the rainfall SQL for a single sensor and datetime range. Note that all
    kwargs are derived from trusted internal sources (none are direct from the end-user).
    """

    tablename = postgres_table_model.objects.model._meta.db_table
    
    query = """
        SELECT
            sq1.id,
            date_trunc('month', sq1.all_ts) as ts,
            sum(val) as val
        from (
            select 
                '{0}'::text as id,
                rr.timestamp as all_ts,
                (rr.data->'{0}'->0)::float as val
            from {1} rr
            where (timestamp >= %s and timestamp <= %s) order by timestamp
        ) sq1
        group by sq1.id, ts
        order by ts;
    """.format(
        sensor_id,
        tablename
    )

    query_params = [
        all_datetimes[0],
        all_datetimes[-1],
    ]

    queryset = postgres_table_model.objects.raw(query, query_params).iterator()

    rows = [
        dict(
            ts=r.ts.astimezone(TZ).isoformat(),
            id=str(r.id),
            val=r.val,
            src=""
        )
        for r in queryset
    ]

    return rows