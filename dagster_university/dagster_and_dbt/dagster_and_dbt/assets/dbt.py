import dagster as dg
from dagster_and_dbt.project import dbt_project
from dagster_dbt import DbtCliResource, dbt_assets


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_analytics(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
