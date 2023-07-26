import dlt

from browsie import job


def load_job() -> None:
    """Loads airflow events. Shows incremental loading. Forces anonymous access token"""
    pipeline = dlt.pipeline("browsie", destination="duckdb", dataset_name="job")
    data = job({"URL": "https://unrealists.com", "delayMS": 3000})
    print(pipeline.run(data))


if __name__ == "__main__":
    load_job()
