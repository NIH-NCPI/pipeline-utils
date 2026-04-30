from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional
from dbt_pipeline_utils.p_utils.files import normalize_name


@dataclass
class StructureContext:
    """
    Centralized, validated context object for structure generation.

    Public API via properties.
    Internal storage uses underscore-backed fields.
    """

    # Required fields (constructor args)
    table_name: str
    df_identifiers: List[str]
    dataset_id: str
    dd_identifier: str
    dd_format: str
    study_id: str
    project_id: str
    project_name: str
    dag_id: str
    pipeline_structure: str
    db_profile: str
    int_model_name: str
    int_model_prefix: str
    combined_model_prefix: str
    int_format: str
    src_tables: List[str]
    int_tables: List[str]
    exp_tables: List[str]
    exp_model_name: str
    exp_model_prefix: str
    exp_format: str
    study_tables: List[str]
    src_model_type: str
    stb_model_type: str
    int_model_type: str
    exp_model_type: str

    # Optional / defaulted fields
    study_config_path: Optional[Path] = None
    study_data_dir: Optional[Path] = None
    pipeline_data_dir: Optional[Path] = None

    # Derived / internal fields
    _study_schema_prefix: str = field(init=False, default="")
    _src_table_prefix: str = field(init=False, default="")

    _stb_table_prefix: str = field(init=False, default="")

    _int_table_prefix: str = field(init=False, default="")
    _int_gen_dd_name: str = field(init=False, default="")
    _int_stg_additions_name: str = field(init=False, default="")
    _src_prefixed_tables: List[str] = field(init=False, default="")
    _stb_prefixed_tables: List[str] = field(init=False, default="")

    _combined_table_prefix: str = field(init=False, default="")
    _combined_prefixed_tables: List[str] = field(init=False, default="")

    _exp_table_prefix: str = field(init=False, default="")
    _exp_prefixed_tables: List[str] = field(init=False, default="")

    def __post_init__(self):
        # Convert paths to Path objects
        if self.study_config_path:
            self._study_config_path = Path(self.study_config_path)
        if self.study_data_dir:
            self._study_data_dir = Path(self.study_data_dir)
        if self.pipeline_data_dir:
            self._pipeline_data_dir = Path(self.pipeline_data_dir)

        structure_key = self.pipeline_structure or self.project_id

        self._study_schema_prefix = normalize_name(
            [structure_key, self.study_id], trailing=False, extension="drop"
        )

        self._src_table_prefix = normalize_name([structure_key, self.study_id, 'src'], trailing=True, extension='drop')
        self._src_prefixed_tables = [
            f"{self._src_table_prefix}{t}" for t in self.src_tables
        ]

        self._stb_table_prefix = normalize_name(
            [structure_key, self.study_id, "stb"], trailing=True, extension="drop"
        )
        self._stb_prefixed_tables = [
            f"{self._stb_table_prefix}{t}" for t in self.int_tables
        ]

        self._int_table_prefix = normalize_name(
            self.int_model_prefix,
            trailing=True,
            extension="drop",
        )
        self._int_gen_dd_name = normalize_name([self.table_name, 'stg_dd.csv'], trailing=False, extension='keep')
        self._int_stg_additions_name = normalize_name([self.table_name, 'stg_additions_dd.csv'], trailing=False, extension='keep')
        self._int_prefixed_tables = [
            f'{self._int_table_prefix}{t}' for t in self.int_tables
        ]

        self._combined_table_prefix = normalize_name(
            self.combined_model_prefix,
            trailing=True,
            extension="drop",
        )
        self._combined_prefixed_tables = [
            f"{self._combined_table_prefix}{t}" for t in self.int_tables
        ]

        self._exp_table_prefix = normalize_name(
            self.exp_model_prefix,
            trailing=True,
            extension="drop",
        )
        self._exp_prefixed_tables = [
            f"{self._exp_table_prefix}{t}" for t in self.exp_tables
        ]

        # Ensure dag_id property is set (calls setter, sets _dag_id)
        self.dag_id = getattr(self, 'dag_id', None) or f"{self.study_id}_dbt_dag"

    # Properties / getters & setters
    @property
    def table_name(self) -> str:
        return self._table_name

    @table_name.setter
    def table_name(self, value: str):
        if not value:
            raise ValueError("table_name cannot be empty")
        self._table_name = value

    @property
    def df_identifiers(self) -> List[str]:
        return self._df_identifiers

    @df_identifiers.setter
    def df_identifiers(self, value: str):
        if not value:
            raise ValueError("df_identifiers cannot be empty")
        self._df_identifiers = value

    @property
    def dataset_id(self) -> List[str]:
        return self._dataset_id

    @dataset_id.setter
    def dataset_id(self, value: str):
        if not value:
            raise ValueError("dataset_id cannot be empty")
        self._dataset_id = value

    @property
    def dd_identifier(self) -> str:
        return self._dd_identifier

    @dd_identifier.setter
    def dd_identifier(self, value: str):
        if not value:
            raise ValueError("dd_identifier cannot be empty")
        self._dd_identifier = value

    @property
    def dd_format(self) -> str:
        return self._dd_format

    @dd_format.setter
    def dd_format(self, value: str):
        if not value:
            raise ValueError("dd_format cannot be empty")
        self._dd_format = value

    @property
    def study_id(self) -> str:
        return self._study_id

    @study_id.setter
    def study_id(self, value: str):
        if not value:
            raise ValueError("study_id cannot be empty")
        self._study_id = value
        # self._recompute_prefixes()

    @property
    def project_id(self) -> str:
        return self._project_id

    @project_id.setter
    def project_id(self, value: str):
        if not value:
            raise ValueError("project_id cannot be empty")
        self._project_id = value

    @property
    def project_name(self) -> str:
        return self._project_name

    @project_name.setter
    def project_name(self, value: str):
        if not value:
            raise ValueError("project_name cannot be empty")
        self._project_name = value

    @property
    def dag_id(self) -> str:
        return self._dag_id     
    
    @dag_id.setter
    def dag_id(self, value: str):
        if not value:
            raise ValueError("dag_id cannot be empty")
        self._dag_id = value

    @property
    def pipeline_structure(self) -> str:
        return self._pipeline_structure

    @pipeline_structure.setter
    def pipeline_structure(self, value: str):
        self._pipeline_structure = value or "inc"

    @property
    def db_profile(self) -> str:
        return self._db_profile

    @db_profile.setter
    def db_profile(self, value: str):
        if not value:
            raise ValueError("db_profile cannot be empty")
        self._db_profile = value

    @property
    def int_model_name(self) -> str:
        return self._int_model_name

    @int_model_name.setter
    def int_model_name(self, value: str):
        self._int_model_name = value

    @property
    def int_model_prefix(self) -> str:
        return self._int_model_prefix

    @int_model_prefix.setter
    def int_model_prefix(self, value: str):
        self._int_model_prefix = value

    @property
    def int_format(self) -> str:
        return self._int_format

    @int_format.setter
    def int_format(self, value: str):
        self._int_format = value

    @property
    def combined_model_prefix(self) -> str:
        return self._combined_model_prefix

    @combined_model_prefix.setter
    def combined_model_prefix(self, value: str):
        self._combined_model_prefix = value or "combined"

    @property
    def src_tables(self) -> str:
        return self._src_tables

    @src_tables.setter
    def src_tables(self, value: str):
        self._src_tables = value

    @property
    def int_tables(self) -> str:
        return self._int_tables

    @int_tables.setter
    def int_tables(self, value: str):
        self._int_tables = value

    @property
    def exp_tables(self) -> str:
        return self._exp_tables

    @exp_tables.setter
    def exp_tables(self, value: str):
        self._exp_tables = value

    @property
    def exp_model_name(self) -> str:
        return self._exp_model_name

    @exp_model_name.setter
    def exp_model_name(self, value: str):
        self._exp_model_name = value

    @property
    def exp_model_prefix(self) -> str:
        return self._exp_model_prefix

    @exp_model_prefix.setter
    def exp_model_prefix(self, value: str):
        self._exp_model_prefix = value

    @property
    def exp_format(self) -> str:
        return self._exp_format

    @exp_format.setter
    def exp_format(self, value: str):
        self._exp_format = value

    @property
    def study_tables(self) -> List[str]:
        return self._study_tables

    @study_tables.setter
    def study_tables(self, value: List[str]):
        if not isinstance(value, list):
            raise TypeError("study_tables must be a list")
        self._study_tables = value
    @property
    def src_model_type(self) -> str:
        return self._src_model_type

    @src_model_type.setter
    def src_model_type(self, value: str):
        self._src_model_type = value

    @property
    def stb_model_type(self) -> str:
        return self._stb_model_type

    @stb_model_type.setter
    def stb_model_type(self, value: str):
        self._stb_model_type = value

    @property
    def int_model_type(self) -> str:
        return self._int_model_type

    @int_model_type.setter
    def int_model_type(self, value: str):
        self._int_model_type = value

    @property
    def exp_model_type(self) -> str:
        return self._exp_model_type

    @exp_model_type.setter
    def exp_model_type(self, value: str):
        self._exp_model_type = value

    @property
    def study_config_path(self) -> Optional[Path]:
        return self._study_config_path

    @study_config_path.setter
    def study_config_path(self, value: Optional[Path]):
        self._study_config_path = Path(value) if value else None

    @property
    def study_data_dir(self) -> Optional[Path]:
        return self._study_data_dir

    @study_data_dir.setter
    def study_data_dir(self, value: Optional[Path]):
        self._study_data_dir = Path(value) if value else None

    @property
    def pipeline_data_dir(self) -> Optional[Path]:
        return self._pipeline_data_dir

    @pipeline_data_dir.setter
    def pipeline_data_dir(self, value: Optional[Path]):
        self._pipeline_data_dir = Path(value) if value else None

    # Derived / read-only properties
    @property
    def src_table_prefix(self) -> str:
        return self._src_table_prefix

    @property
    def study_schema_prefix(self) -> str:
        return self._study_schema_prefix

    @property
    def src_schema(self) -> str:
        return f"{self._study_schema_prefix}_src"

    @property
    def int_schema(self) -> str:
        return f"{self._study_schema_prefix}_int"

    @property
    def stb_schema(self) -> str:
        return f"{self._study_schema_prefix}_stb"

    @property
    def access_schema(self) -> str:
        return self.int_model_name

    @property
    def export_schema(self) -> str:
        return f"{self.exp_model_name}_export"

    @property
    def combined_schema(self) -> str:
        return "combined"

    @property
    def default_materialized(self) -> str:
        return "table"

    @property
    def src_prefixed_tables(self) -> str:
        return self._src_prefixed_tables

    @property
    def stb_table_prefix(self) -> str:
        return self._stb_table_prefix

    @property
    def stb_prefixed_tables(self) -> str:
        return self._stb_prefixed_tables

    @property
    def int_table_prefix(self) -> str:
        return self._int_table_prefix

    @property
    def int_gen_dd_name(self) -> str:
        return self._int_gen_dd_name

    @property
    def int_stg_additions_name(self) -> str:
        return self._int_stg_additions_name

    @property
    def int_prefixed_tables(self) -> str:
        return self._int_prefixed_tables

    @property
    def exp_table_prefix(self) -> str:
        return self._exp_table_prefix

    @property
    def exp_prefixed_tables(self) -> str:
        return self._exp_prefixed_tables

    @property
    def combined_table_prefix(self) -> str:
        return self._combined_table_prefix

    @property
    def combined_prefixed_tables(self) -> str:
        return self._combined_prefixed_tables

    @property
    def study_config_dir(self) -> Optional[Path]:
        return self._study_config_path.parent if self._study_config_path else None

    # Validation
    def validate(self) -> None:
        required = {
            "dd_identifier": self._dd_identifier,
            "study_id": self._study_id,
            "project_id": self._project_id,
            "project_name": self._project_name,
            "dag_id": self._dag_id,
            "db_profile": self._db_profile,
            "study_tables": self._study_tables,
            "int_model_name": self._int_model_name,
            "int_model_prefix": self._int_model_prefix,
            "exp_model_name": self._exp_model_name,
            "exp_model_prefix": self._exp_model_prefix,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(f"Missing required StructureContext fields: {missing}")
