from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path


@dataclass(frozen=True)
class DataDictionaryConfig:
    identifier: str
    import_type: str
    format: str


@dataclass(frozen=True)
class DataFileConfig:
    identifiers: List[str]
    import_type: str
    join_cols: Dict[str, str]


@dataclass(frozen=True)
class DbtProjectDefaults:
    schema: Optional[str] = None
    materialized: Optional[str] = None


    def dbt_dict(self) -> dict:
        data = {}

        if self.schema is not None:
            data["+schema"] = self.schema

        if self.materialized is not None:
            data["+materialized"] = self.materialized

        return data


@dataclass(frozen=True)
class StudyConfig:
    study_id: str
    project_id: str
    db_profile: str
    pipeline_db: str
    project_structure: str
    tgt_model_id: str
    data_dir: Path
    int_model_id: str
    study_tables: list[str]


    data_file: Dict[str, DataFileConfig]
    data_dictionary: Dict[str, DataDictionaryConfig]
    dbt_proj_config: Dict[str, DbtProjectDefaults]

    @classmethod
    def from_dict(cls, raw: dict) -> "StudyConfig":
        return cls(
            study_id=raw["study_id"],
            project_id=raw["project_id"],
            db_profile=raw["db_profile"],
            pipeline_db=raw["pipeline_db"],
            project_structure=raw["project_structure"],
            tgt_model_id=raw["tgt_model_id"],
            data_dir=Path(raw["data_dir"]),
            int_model_id=raw["int_model_id"],
            study_tables= list(raw["data_file"].keys()),
            data_file={
                name: DataFileConfig(
                    identifiers=cfg["identifier"],
                    import_type=cfg["import_type"],
                    join_cols=cfg["join_cols"],
                )
                for name, cfg in raw["data_file"].items()
            },
            data_dictionary={
                name: DataDictionaryConfig(
                    identifier=cfg["identifier"],
                    import_type=cfg["import_type"],
                    format=cfg["format"],
                )
                for name, cfg in raw["data_dictionary"].items()
            },
            dbt_proj_config={
                name: DbtProjectDefaults(
                    schema=cfg["schema"],
                    materialized=cfg["materialized"],
                )
                for name, cfg in raw["dbt_project"].items()
            },
        )

'''
Internal Configs are initiallized in the PipelineObject after 'paths'.
'''
@dataclass(frozen=True)
class InternalDataDictionary:
    identifier: Path
    pipeline_identifier: Path
    table_id: str


@dataclass(frozen=True)
class InternalConfig:
    data_dictionary: Dict[str, InternalDataDictionary]
    int_tables: list[str]

    @classmethod
    def from_dict(cls, raw: dict, int_table_prefix: str) -> "InternalConfig":
        data_dict: Dict[str, InternalDataDictionary] = {}

        for name, cfg in raw["data_dictionary"].items():
            data_dict[name] = InternalDataDictionary(
                identifier=Path(cfg["identifier"]),
                pipeline_identifier=Path(cfg["pipeline_identifier"]),
                table_id=f"{int_table_prefix}_{name}",
            )

        int_tables = [v.table_id for v in data_dict.values()]

        return cls(
            data_dictionary=data_dict,
            int_tables=int_tables,
        )
