from dataclasses import dataclass
from typing import List, Dict, Optional, Any
from pathlib import Path
from dbt_pipeline_utils.p_utils.general import normalize_name


@dataclass(frozen=True)
class StudyPipelineConfig:
    db: str
    structure: str
    pipeline_data_dir: str
    int_model_name: str
    exp_model_name: str
    src_model_type: str
    stb_model_type: str
    int_model_type: str
    exp_model_type: str


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
    vars: Optional[Dict[str, Any]] = None

    def dbt_dict(self) -> dict:
        data = {}

        if self.schema is not None:
            data["+schema"] = self.schema

        if self.materialized is not None:
            data["+materialized"] = self.materialized

        if self.vars is not None:
            data["vars"] = self.vars

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
    dataset_id: str
    consent_id: str

@dataclass(frozen=True)
class StudyConfig:
    study_tables: list[str]
    src_tables: list[str]
    pipeline: StudyPipelineConfig
    study: StudyStudyConfig
    dbt_proj_config: Dict[str, StudyDbtProjectDefaults]
    data_file: Dict[str, StudyDataFileConfig]
    data_dictionary: Dict[str, StudyDataDictionaryConfig]

    @classmethod
    def from_dict(cls, raw: dict) -> "StudyConfig":

        return cls(
            study_tables=list(raw["data_file"].keys()),
            src_tables=[
                item
                for table in raw["data_file"].values()
                for item in table["identifier"]
            ],
            pipeline=StudyPipelineConfig(
                db=raw["pipeline"]["db"],
                structure=raw["pipeline"]["structure"],
                pipeline_data_dir=raw["pipeline"]["data_dir"],
                int_model_name=raw["pipeline"]["int_model_name"],
                exp_model_name=raw["pipeline"]["exp_model_name"],
                src_model_type=raw["pipeline"]["model_type"]["src"],
                stb_model_type=raw["pipeline"]["model_type"]["stb"],
                int_model_type=raw["pipeline"]["model_type"]["int"],
                exp_model_type=raw["pipeline"]["model_type"]["exp"],
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
                    vars=cfg.get("vars"),
                )
                for name, cfg in raw["dbt_project"].items()
            },
            data_file={
                name: StudyDataFileConfig(
                    identifiers=cfg["identifier"],
                    import_type=cfg["import_type"],
                    dataset_id=cfg["dataset_id"],
                    consent_id=cfg["consent_id"],
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
    int_dd_identifier: Path


@dataclass(frozen=True)
class InternalConfig:
    model_name: str
    model_prefix: str
    int_format: str
    data_dictionary: Dict[str, InternalDataDictionary]
    int_tables: list[str]

    @classmethod
    def from_dict(cls, raw: dict) -> "InternalConfig":
        model_name = normalize_name(raw["model_name"], trailing=False, extension="drop")
        model_prefix = normalize_name(raw["model_prefix"], trailing=False, extension='drop')
        int_format = raw["format"]

        data_dict: Dict[str, InternalDataDictionary] = {
            name: InternalDataDictionary(
                identifier=Path(cfg["identifier"]),
                int_dd_identifier=Path(
                    normalize_name([model_prefix, cfg["identifier"]], trailing=False, extension='keep')
                ),
            )
            for name, cfg in raw["data_dictionary"].items()
        }

        int_tables = [
            normalize_name(
                str(v.identifier).replace("-dd", ""), trailing=False, extension="drop"
            )
            for v in data_dict.values()
        ]

        return cls(
            model_name=model_name,
            model_prefix=model_prefix,
            int_format=int_format,
            data_dictionary=data_dict,
            int_tables=int_tables,
        )


@dataclass(frozen=True)
class ExportDataDictionary:
    identifier: Path
    exp_dd_identifier: Path
@dataclass(frozen=True)
class ExportConfig:
    model_name: str
    model_prefix: str
    exp_format: str
    data_dictionary: Dict[str, ExportDataDictionary]
    exp_tables: list[str]

    @classmethod
    def from_dict(cls, raw: dict) -> "ExportConfig":
        data_dict: Dict[str, ExportDataDictionary] = {}
        model_name = normalize_name(raw["model_name"], trailing=False, extension="drop")
        model_prefix = normalize_name(raw['model_prefix'], trailing=False, extension='drop')
        exp_format = raw["format"]
        for name, cfg in raw["data_dictionary"].items():
            data_dict[name] = ExportDataDictionary(
                identifier=Path(cfg["identifier"]),
                exp_dd_identifier=Path(
                    normalize_name([model_prefix, '_', cfg["identifier"]], trailing=False, extension='keep')
                ),
            )

        exp_tables = [
            normalize_name(
                str(v.identifier).replace("-dd", ""), trailing=False, extension="drop"
            )
            for v in data_dict.values()
        ]

        return cls(
            model_name=model_name,
            model_prefix=model_prefix,
            exp_format=exp_format,
            data_dictionary=data_dict,
            exp_tables=exp_tables,
        )
