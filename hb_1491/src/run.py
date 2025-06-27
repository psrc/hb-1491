import configuration
import yaml
from pathlib import Path
import get_stops
import capacity_analysis_1491

file = Path().joinpath(configuration.args.configs_dir, "config.yaml")

config = yaml.safe_load(open(file))

dfs = None
if config["run_get_transit_stops"]:
    dfs = get_stops.run(config)
if config["run_capacity_analysis"]:
    capacity_analysis_1491.run(config)
    print("done capacity analysis")
