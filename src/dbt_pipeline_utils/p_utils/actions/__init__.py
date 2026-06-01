from pathlib import Path
from dbt_pipeline_utils import find_repo_root
from dbt_pipeline_utils.p_utils.factory_functions import build_pipeline_objects
from dbt_pipeline_utils.p_utils.configs import StudyConfig
from dbt_pipeline_utils.p_utils.files import read_file

def resolve_static_path(static_data_dir=None):
    """Resolve the static data directory path, using repo root or cwd as needed."""
    root_path = find_repo_root()
    if static_data_dir is None:
        return (Path.cwd() / "data").resolve()
    else:
        return (root_path / static_data_dir).resolve()


def resolve_study_config_path(config_path):
    """Resolve a study config path from absolute, repo-root, or cwd-relative input."""
    path = Path(config_path)
    if path.is_absolute():
        return path.resolve()

    root_path = find_repo_root()
    rooted = (root_path / path).resolve()
    if rooted.exists():
        return rooted

    cwd_path = (Path.cwd() / path).resolve()
    if cwd_path.exists():
        return cwd_path

    return rooted

def load_study_config(config_path):
    """Load and parse a study config YAML using shared path resolution."""
    study_config_path = resolve_study_config_path(config_path)
    raw_config = read_file(study_config_path)
    study_config = StudyConfig.from_dict(raw_config)
    return study_config, study_config_path


def load_pipeline(config_path, static_data_dir=None):
    """Load a study config YAML and return fully-initialised pipeline objects."""
    study_config, study_config_path = load_study_config(config_path)
    static_path = resolve_static_path(static_data_dir)
    return build_pipeline_objects(static_path, study_config, study_config_path)


def filter_tables(pipeline_objects, tables=None):
    """Return *pipeline_objects* filtered to the given table names.

    If *tables* is ``None`` or empty, all objects are returned unchanged.
    """
    if not tables:
        return pipeline_objects
    filtered = {k: v for k, v in pipeline_objects.items() if k in tables}
    if not filtered:
        available = list(pipeline_objects.keys())
        raise ValueError(
            f"Table(s) {tables} not found in config. Available: {available}"
        )
    return filtered
