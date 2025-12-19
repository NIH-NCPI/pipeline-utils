from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path


@dataclass(frozen=True)
class StudyPipelineConfig:
    db: str
    structure: str
    pipeline_data_dir: str
    int_model_name: str
    exp_model_name: str


@dataclass(frozen=True)
class StudyStudyConfig:
    study_id: str
    project_id: str
    db_profile: str
    study_data_dir: str


@dataclass(frozen=True)
class StudyDbtProjectDefaults:
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
class StudyDataDictionaryConfig:
    identifier: str
    import_type: str
    format: str


@dataclass(frozen=True)
class StudyDataFileConfig:
    identifiers: List[str]
    import_type: str
    join_cols: Dict[str, str]


@dataclass(frozen=True)
class StudyConfig:
    study_tables: list[str]
    pipeline: StudyPipelineConfig
    study: StudyStudyConfig
    dbt_proj_config: Dict[str, StudyDbtProjectDefaults]
    data_file: Dict[str, StudyDataFileConfig]
    data_dictionary: Dict[str, StudyDataDictionaryConfig]

    @classmethod
    def from_dict(cls, raw: dict) -> "StudyConfig":
        return cls(
            study_tables=list(raw["data_file"].keys()),
            pipeline=StudyPipelineConfig(
                db=raw["pipeline"]["db"],
                structure=raw["pipeline"]["structure"],
                pipeline_data_dir=raw["pipeline"]["data_dir"],
                int_model_name=raw["pipeline"]["int_model_name"],
                exp_model_name=raw["pipeline"]["exp_model_name"],
            ),
            study=StudyStudyConfig(
                study_id=raw["study"]["study_id"],
                project_id=raw["study"]["project_id"],
                db_profile=raw["study"]["db_profile"],
                study_data_dir=raw["study"]["data_dir"],
            ),
            dbt_proj_config={
                name: StudyDbtProjectDefaults(
                    schema=cfg.get("schema"),
                    materialized=cfg.get("materialized"),
                )
                for name, cfg in raw["dbt_project"].items()
            },
            data_file={
                name: StudyDataFileConfig(
                    identifiers=cfg["identifier"],
                    import_type=cfg["import_type"],
                    join_cols=cfg["join_cols"],
                )
                for name, cfg in raw["data_file"].items()
            },
            data_dictionary={
                name: StudyDataDictionaryConfig(
                    identifier=cfg["identifier"],
                    import_type=cfg["import_type"],
                    format=cfg["format"],
                )
                for name, cfg in raw["data_dictionary"].items()
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
    model_name: str
    model_prefix: str
    data_dictionary: Dict[str, InternalDataDictionary]
    int_tables: list[str]

    @classmethod
    def from_dict(cls, raw: dict) -> "InternalConfig":
        model_name = raw["model_name"]
        model_prefix = raw["model_prefix"]

        data_dict: Dict[str, InternalDataDictionary] = {
            name: InternalDataDictionary(
                identifier=Path(cfg["identifier"]),
                pipeline_identifier=Path(cfg["pipeline_identifier"]),
                table_id=f"{model_prefix}_{name}",
            )
            for name, cfg in raw["data_dictionary"].items()
        }

        return cls(
            model_name=model_name,
            model_prefix=model_prefix,
            data_dictionary=data_dict,
            int_tables=[v.table_id for v in data_dict.values()],
        )



# TODO tgt model id to name use exp everywhere.
# prefixes to configs and used to create table names

@dataclass(frozen=True)
class ExportDataDictionary:
    identifier: Path
    pipeline_identifier: Path
    table_id: str
@dataclass(frozen=True)
class ExportConfig:
    model_name: str
    model_prefix: str
    data_dictionary: Dict[str, ExportDataDictionary]
    exp_tables: list[str]

    @classmethod
    def from_dict(cls, raw: dict) -> "ExportConfig":
        data_dict: Dict[str, ExportDataDictionary] = {}
        model_name = raw['model_name']
        model_prefix = raw['model_prefix']
        for name, cfg in raw["data_dictionary"].items():
            data_dict[name] = ExportDataDictionary(
                identifier=Path(cfg["identifier"]),
                pipeline_identifier=Path(cfg["pipeline_identifier"]),
                table_id=f"{model_prefix}_{name}",
            )

        exp_tables = [v.table_id for v in data_dict.values()]

        return cls(
            model_name=model_name,
            model_prefix=model_prefix,
            data_dictionary=data_dict,
            exp_tables=exp_tables,
        )
