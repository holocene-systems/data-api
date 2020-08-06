
def query_pgdb(postgres_table_model, sensors_ids, start_dt, end_dt):

    tablename = postgres_table_model.objects.model._meta.db_table

    queryset = postgres_table_model.objects.filter(
        Q(timestamp__gte=start_dt),
        Q(timestamp__lte=end_dt)
    )

    rows = postgres_table_model.as_dataframe(queryset)
    rows.timestamp = rows.timestamp.map(lambda t: t.isoformat())