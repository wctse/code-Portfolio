from dagster import fs_io_manager
from pipelines.shared.resources.io_managers.parquet_io_manager import local_partitioned_parquet_io_manager

base_path = "./artifacts"

RESOURCES_DEV = {
    "io_manager": local_partitioned_parquet_io_manager.configured({"base_path": base_path}),
    "fs_io_manager": fs_io_manager.configured({"base_dir": base_path}),
}
