import os
from dagster import (
    Definitions,
    get_dagster_logger,
    multiprocess_executor,
)

from pipelines.core.extract.assets.chain import factory_outputs as chain_factory_outputs
from pipelines.core.extract.resources import RESOURCES_DEV

all_assets = [
    *chain_factory_outputs.assets,
]

all_jobs = [
    *chain_factory_outputs.jobs,
]

all_sensors = [
    *chain_factory_outputs.sensors,
]

logger = get_dagster_logger()
resource_defs_by_deployment_name = {
    "dev": RESOURCES_DEV,
}

deployment_name = os.environ.get("ENVIRONMENT", "dev")

logger.info(f"Using deployment {deployment_name}")
resource_defs = resource_defs_by_deployment_name[deployment_name]

defs = Definitions(
    jobs=all_jobs,
    assets=all_assets,
    resources=resource_defs,
    sensors=all_sensors,
    executor=multiprocess_executor,
)
