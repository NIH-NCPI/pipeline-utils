from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from dbt_pipeline_utils.p_utils.general import normalize_name
from dbt_pipeline_utils.p_utils.structures.project_structure import StructureBC


@dataclass
class DatabaseContext:
    """
    Centralized, validated context object for database attributes.

    Public API via properties.
    Internal storage uses underscore-backed fields.
    """

    # Required fields (constructor args)
    structure: StructureBC
    paths: Dict[str, Path] = field(init=False)

    # Private fields initialized from StructureBC
    _src_table_prefix: str = field(init=False)
    _int_table_prefix: str = field(init=False)
    _exp_table_prefix: str = field(init=False)

    _df_identifiers: List[str] = field(init=False)
    _dd_identifier: str = field(init=False)
    _study_id: str = field(init=False)
    _project_id: str = field(init=False)
    _table_name: str = field(init=False)

    def __post_init__(self) -> None:
        """Initialize fields based on the StructureBC instance."""
        # Initialize paths from StructureBC
        self.paths = self.structure.get_paths()

        # Initialize fields from StructureBC
        self._src_table_prefix = self.structure.src_table_prefix
        self._int_table_prefix = self.structure.int_table_prefix
        self._exp_table_prefix = self.structure.exp_table_prefix

        self._df_identifiers = self.structure.df_identifiers
        self._dd_identifier = self.structure.dd_identifier
        self._study_id = self.structure.study_id
        self._project_id = self.structure.project_id
        self._table_name = self.structure.table_name

    # Properties to expose private fields
    @property
    def src_table_prefix(self) -> str:
        return self._src_table_prefix

    @property
    def int_table_prefix(self) -> str:
        return self._int_table_prefix

    @property
    def exp_table_prefix(self) -> str:
        return self._exp_table_prefix

    @property
    def df_identifiers(self) -> List[str]:
        return self._df_identifiers

    @property
    def dd_identifier(self) -> str:
        return self._dd_identifier

    @property
    def project_id(self) -> str:
        return self._project_id

    @property
    def study_id(self) -> str:
        return self._study_id

    @property
    def table_name(self) -> str:
        return self._table_name

    #     @table_name.setter
    #     def table_name(self, value: str):
    #         if not value:
    #             raise ValueError("table_name cannot be empty")
    #         self._table_name = value

    #     @df_identifiers.setter
    #     def df_identifiers(self, value: str):
    #         if not value:
    #             raise ValueError("df_identifiers cannot be empty")
    #         self._df_identifiers = value

    #     @dd_identifier.setter
    #     def dd_identifier(self, value: str):
    #         if not value:
    #             raise ValueError("dd_identifier cannot be empty")
    #         self._dd_identifier = value

    #     @property
    #     def dd_format(self) -> str:
    #         return self._dd_format

    #     @dd_format.setter
    #     def dd_format(self, value: str):
    #         if not value:
    #             raise ValueError("dd_format cannot be empty")
    #         self._dd_format = value

    #     @study_id.setter
    #     def study_id(self, value: str):
    #         if not value:
    #             raise ValueError("study_id cannot be empty")
    #         self._study_id = value
    #         # self._recompute_prefixes()

    #     @project_id.setter
    #     def project_id(self, value: str):
    #         if not value:
    #             raise ValueError("project_id cannot be empty")
    #         self._project_id = value

    #     @property
    #     def db_profile(self) -> str:
    #         return self._db_profile

    #     @db_profile.setter
    #     def db_profile(self, value: str):
    #         if not value:
    #             raise ValueError("db_profile cannot be empty")
    #         self._db_profile = value

    #     @property
    #     def int_model_name(self) -> str:
    #         return self._int_model_name

    #     @int_model_name.setter
    #     def int_model_name(self, value: str):
    #         self._int_model_name = value

    #     @property
    #     def int_model_prefix(self) -> str:
    #         return self._int_model_prefix

    #     @int_model_prefix.setter
    #     def int_model_prefix(self, value: str):
    #         self._int_model_prefix = value
    #         # self._recompute_prefixes()

    #     @property
    #     def exp_model_name(self) -> str:
    #         return self._exp_model_name

    #     @exp_model_name.setter
    #     def exp_model_name(self, value: str):
    #         self._exp_model_name = value

    #     @property
    #     def exp_model_prefix(self) -> str:
    #         return self._exp_model_prefix

    #     @exp_model_prefix.setter
    #     def exp_model_prefix(self, value: str):
    #         self._exp_model_prefix = value

    #     @property
    #     def study_tables(self) -> List[str]:
    #         return self._study_tables

    #     @study_tables.setter
    #     def study_tables(self, value: List[str]):
    #         if not isinstance(value, list):
    #             raise TypeError("study_tables must be a list")
    #         self._study_tables = value

    #     @property
    #     def study_config_path(self) -> Optional[Path]:
    #         return self._study_config_path

    #     @study_config_path.setter
    #     def study_config_path(self, value: Optional[Path]):
    #         self._study_config_path = Path(value) if value else None

    #     @property
    #     def study_data_dir(self) -> Optional[Path]:
    #         return self._study_data_dir

    #     @study_data_dir.setter
    #     def study_data_dir(self, value: Optional[Path]):
    #         self._study_data_dir = Path(value) if value else None

    #     @property
    #     def pipeline_data_dir(self) -> Optional[Path]:
    #         return self._pipeline_data_dir

    #     @pipeline_data_dir.setter
    #     def pipeline_data_dir(self, value: Optional[Path]):
    #         self._pipeline_data_dir = Path(value) if value else None

    #     @property
    #     def src_dbtp_def(self) -> Dict[str, Any]:
    #         return self._src_dbtp_def

    #     @src_dbtp_def.setter
    #     def src_dbtp_def(self, value: Dict[str, Any]):
    #         self._src_dbtp_def = value or {}

    #     @property
    #     def int_dbtp_def(self) -> Dict[str, Any]:
    #         return self._int_dbtp_def

    #     @int_dbtp_def.setter
    #     def int_dbtp_def(self, value: Dict[str, Any]):
    #         self._int_dbtp_def = value or {}

    #     @property
    #     def exp_dbtp_def(self) -> Dict[str, Any]:
    #         return self._exp_dbtp_def

    #     @exp_dbtp_def.setter
    #     def exp_dbtp_def(self, value: Dict[str, Any]):
    #         self._exp_dbtp_def = value or {}

    # Derived / read-only properties


#     @property
#     def int_gen_dd_name(self) -> str:
#         return self._int_gen_dd_name

#     @property
#     def int_stg_additions_name(self) -> str:
#         return self._int_stg_additions_name

#     @property
#     def study_config_dir(self) -> Optional[Path]:src/dbt_pipeline_utils/p_utils/pipeline_generation
#         return self._study_config_path.parent if self._study_config_path else None

#     # Validation
#     def validate(self) -> None:
#         required = {
#             "dd_identifier": self._dd_identifier,
#             "study_id": self._study_id,
#             "project_id": self._project_id,
#             "db_profile": self._db_profile,
#             "study_tables": self._study_tables,
#             "int_model_name": self._int_model_name,
#             "int_model_prefix": self._int_model_prefix,
#             "exp_model_name": self._exp_model_name,
#             "exp_model_prefix": self._exp_model_prefix,
#         }
#         missing = [k for k, v in required.items() if not v]
#         if missing:
#             raise RuntimeError(f"Missing required StructureContext fields: {missing}")
