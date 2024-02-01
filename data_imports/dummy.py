import dlt
import json


data = [
    {
        "id": 1,
        "name": "LeBron James",
        "position": "F",
        "height_feet": 6,
    },
    {
        "id": 2,
        "name": "Kevin Durant",
        "position": "F",
        "height_feet": 7,
    },
    {
        "id": 3,
        "name": "Stephen Curry",
        "position": "G",
        "height_feet": 5,
        "accomplishments": [
            {"year": 2015, "award": "MVP"},
            {"year": 2015, "award": "Champion"},
            {"year": 2016, "award": "MVP"},
            {"year": 2017, "award": "MVP"},
        ],
    },
]

pipeline = dlt.pipeline(
    pipeline_name="dwh",
    destination=dlt.destinations.duckdb(credentials="database/dwh.duckdb"),
    dataset_name="raw_dummy",
    import_schema_path="data_imports/schemas/import/",
    export_schema_path="data_imports/schemas/export",
)

load_info = pipeline.run(
    data, table_name="players", write_disposition="replace", primary_key="id"
)
print("done")
