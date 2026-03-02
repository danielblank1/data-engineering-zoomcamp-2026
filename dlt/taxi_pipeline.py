import dlt
from dlt.sources.rest_api import rest_api_source


def taxi_source():
    """Create a dlt REST API source for NYC taxi trip data."""
    source = rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
            },
            "resources": [
                {
                    "name": "rides",
                    "endpoint": {
                        "path": "data_engineering_zoomcamp_api",
                        "paginator": {
                            "type": "page_number",
                            "base_page": 1,
                            "page_param": "page",
                            "total_path": None,
                            "maximum_page": None,
                            "stop_after_empty_page": True,
                        },
                    },
                },
            ],
        }
    )
    return source


def main():
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
    )

    # Load the data
    source = taxi_source()
    load_info = pipeline.run(source)

    # Print load info
    print("=" * 60)
    print("Pipeline load complete!")
    print("=" * 60)
    print(load_info)

    # Show row counts
    with pipeline.sql_client() as client:
        result = client.execute_sql("SELECT COUNT(*) FROM taxi_data.rides")
        print(f"\nTotal rows loaded: {result[0][0]}")


if __name__ == "__main__":
    main()