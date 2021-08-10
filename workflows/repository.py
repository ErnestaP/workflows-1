from dagster import repository

from workflows.pipelines.my_pipeline import my_pipeline
from workflows.schedules.my_hourly_schedule import my_hourly_schedule
from workflows.sensors.my_sensor import my_sensor


@repository
def workflows():
    """
    The repository definition for this workflows Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """
    pipelines = [my_pipeline]
    schedules = [my_hourly_schedule]
    sensors = [my_sensor]

    return pipelines + schedules + sensors
