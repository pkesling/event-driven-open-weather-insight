from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]
DBT_DIR = PROJECT_ROOT / "src" / "weather_insight" / "dbt"


def test_dbt_project_contains_expected_name():
    contents = (DBT_DIR / "dbt_project.yml").read_text()
    assert 'name: "openaq_dbt"' in contents
    assert "macro-paths" in contents


def test_generate_schema_macro_exists():
    macro = (DBT_DIR / "macros" / "schema_names.sql").read_text()
    assert "macro generate_schema_name" in macro
