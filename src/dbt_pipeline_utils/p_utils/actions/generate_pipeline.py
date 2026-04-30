import argparse
from dbt_pipeline_utils.p_utils.actions import load_pipeline
from dbt_pipeline_utils.p_utils.actions.stages import run_all


def main():
    """Legacy entry point: generates all pipeline artifacts from a study config."""

    parser = argparse.ArgumentParser(
        description="Initialize DBT transformation for study data."
    )

    parser.add_argument(
        "-sc",
        "--study_config_filepath",
        required=True,
        help="Path to {study}_study.yaml from root dir. Example 'data/{study_id}/_{study_id}_study.yaml'",
    )
    parser.add_argument(
        "-d",
        "--static_data_dir",
        required=False,
        default=None,
        help="Path to the static data dir - parent dir, from the repo root.",
    )

    args = parser.parse_args()

    pipeline_objects = load_pipeline(args.study_config_filepath, args.static_data_dir)
    run_all(pipeline_objects)
