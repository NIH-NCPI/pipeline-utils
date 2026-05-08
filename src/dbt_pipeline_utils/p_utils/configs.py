from dataclasses import dataclass
from typing import List, Dict, Optional
from pathlib import Path
from dbt_pipeline_utils.p_utils.files import normalize_name


@dataclass(frozen=True)
class StudyStudyConfig:
    db: str
    structure: str
    project_config: str
    pipeline_data_dir: str
    int_model_name: str
    combined_model_prefix: str
    exp_model_name: str
    src_model_type: str
    stb_model_type: str
    int_model_type: str
    exp_model_type: str
    study_id: str
    project_name: str
    project_id: str
    db_profile: str
    study_data_dir: str
    dag_id: str


@dataclass(frozen=True)
class StudyDataDictionaryConfig:
    identifier: str
    format: str


@dataclass(frozen=True)
class StudyDataFileConfig:
    identifiers: List[str]
    dataset_id: str
    consent_id: str

@dataclass(frozen=True)
class StudyConfig:
    study_tables: list[str]
    src_tables: list[str]
    study: StudyStudyConfig
    data_file: Dict[str, StudyDataFileConfig]
    data_dictionary: Dict[str, StudyDataDictionaryConfig]

    @classmethod
    def from_dict(cls, raw: dict) -> "StudyConfig":
        config_raw = raw["study"]

        study_id = config_raw.get("study_id")
        project_name = config_raw.get("project_name")
        project_id = config_raw.get("project_id")
        project_name = config_raw.get("project_name")
        db_profile = config_raw.get("dbt_profile")
        dag_id = config_raw.get("dag_id")
        pipeline_data_dir = config_raw.get("pipeline_data_dir")

        study_data_root = config_raw.get("study_data_dir")
        if study_data_root is None:
            raise KeyError("Missing required config key: study.study_data_dir")

        study_data_path = Path(study_data_root)
        if study_id and study_data_path.name != study_id:
            study_data_path = study_data_path / study_id

        if study_id is None:
            raise KeyError("Missing required config key: study.study_id")
        if project_name is None:
            raise KeyError("Missing required config key: study.project_name")
        if project_id is None:
            raise KeyError("Missing required config key: study.project_id")
        if project_name is None:
            raise KeyError("Missing required config key: study.project_name")
        if db_profile is None:
            raise KeyError("Missing required config key: study.dbt_profile")
        if pipeline_data_dir is None:
            raise KeyError("Missing required config key: study.pipeline_data_dir")

        return cls(
            study_tables=list(raw["data_file"].keys()),
            src_tables=[
                item
                for table in raw["data_file"].values()
                for item in table["identifier"]
            ],
            study=StudyStudyConfig(
                db=config_raw["db"],
                structure=config_raw.get("structure", "inc"),
                project_config=config_raw["project_config"],
                pipeline_data_dir=pipeline_data_dir,
                int_model_name=config_raw["int_model_name"],
                combined_model_prefix=config_raw.get(
                    "combined_model_prefix", "combined"
                ),
                exp_model_name=config_raw["exp_model_name"],
                src_model_type=config_raw["model_type"]["src"],
                stb_model_type=config_raw["model_type"]["stb"],
                int_model_type=config_raw["model_type"]["int"],
                exp_model_type=config_raw["model_type"]["exp"],
                study_id=study_id,
                project_name=project_name,
                project_id=project_id,
                db_profile=db_profile,
                study_data_dir=str(study_data_path),
                dag_id=dag_id,
            ),
            data_file={
                name: StudyDataFileConfig(
                    identifiers=cfg["identifier"],
                    dataset_id=cfg["dataset_id"],
                    consent_id=cfg["consent_id"],
                )
                for name, cfg in raw["data_file"].items()
            },
            data_dictionary={
                name: StudyDataDictionaryConfig(
                    identifier=cfg["identifier"],
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
