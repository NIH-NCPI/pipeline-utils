from pathlib import Path
from typing import Any
from dataclasses import dataclass
from dbt_pipeline_utils import logger
from dbt_pipeline_utils.p_utils.files import normalize_string, write_file



@dataclass
class DagGenerator:
    study_id: str
    project_name: str
    dag_id: str

    @staticmethod
    def _build_dag_file_content(
        project_name: str,
        study_id: str,
        dag_id: str,
        schedule: str,
        target_name: str,
        conn_id: str,
        tag_program: str,
        select_expr: str,
    ) -> str:
        return f'''from airflow.sdk import Variable

from cosmos import (
    DbtDag,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name=Variable.get("DBT_PROFILE_NAME"),
    target_name="{target_name}",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="{conn_id}",
        profile_args={{"schema": "{target_name}"}},
    ),
)

{study_id}_dag = DbtDag(
    project_config=ProjectConfig(
        Variable.get("DBT_PROJECT_DIR"),
        install_dbt_deps=True,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=Variable.get("DBT_EXECUTABLE_PATH"),
    ),
    render_config=RenderConfig(select=["{select_expr}"]),
    schedule="{schedule}",
    dag_id="{dag_id}",
    tags=["{tag_program}", "{project_name}"],
)
'''


    @staticmethod
    def _render_dag_template(value: str, template_vars: dict[str, str]) -> str:
        try:
            return value.format(**template_vars)
        except KeyError as exc:
            missing = exc.args[0]
            raise KeyError(
                f"Missing DAG template variable '{missing}' in value '{value}'. "
                f"Available: {sorted(template_vars.keys())}"
            ) from exc


    def generate_dag_from_study_config(self, dag_dir: str) -> Path:
        """Generate an Airflow Cosmos DAG file from study YAML config."""

        merged: dict[str, Any] = dict()
        template_vars = {
            "study_id": self.study_id,
            "DAG_FILENAME": normalize_string(self.dag_id, extension="drop" )or f"{self.study_id}_dbt_dag",
        }
        # Fill defaults from existing kids_first DAG conventions when omitted in config.
        merged.setdefault("select_expr", f"config.meta.study:{self.study_id}")
        merged.setdefault("schedule", "@daily")
        merged.setdefault("target_name", "prd")
        merged.setdefault("conn_id", "postgres_prd_svc")
        merged.setdefault("tag_program", "POC")
        merged.setdefault("dag_id", template_vars["DAG_FILENAME"])

        dag_id_val = self._render_dag_template(str(merged["dag_id"]), template_vars)
        template_vars["DAG_ID"] = dag_id_val
        select_expr = self._render_dag_template(str(merged["select_expr"]), template_vars)
        schedule = self._render_dag_template(str(merged["schedule"]), template_vars)
        target_name = self._render_dag_template(str(merged["target_name"]), template_vars)
        conn_id = self._render_dag_template(str(merged["conn_id"]), template_vars)
        tag_program = self._render_dag_template(str(merged["tag_program"]), template_vars)

        output_filename = self._render_dag_template(f'{dag_id_val}.py', template_vars)
        output_path = Path(dag_dir) / output_filename

        try:
            write_file(
                output_path,
                self._build_dag_file_content(
                    project_name=self.project_name,
                    study_id=self.study_id,
                    dag_id=dag_id_val,
                    schedule=schedule,
                    target_name=target_name,
                    conn_id=conn_id,
                    tag_program=tag_program,
                    select_expr=select_expr,
                ),
                mode="overwrite",
            )
            logger.info(f"DAG generated: {output_path}")
        except Exception as e:
            logger.error(f"Failed to write DAG file: {e}")
            raise
        return output_path
