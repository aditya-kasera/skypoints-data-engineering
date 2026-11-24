import duckdb
import pandas as pd

import skypoints_data_engineering


def test_package_and_duckdb_pandas_integration() -> None:
    result = duckdb.sql("SELECT 1 AS sky_points_check").df()

    assert skypoints_data_engineering.__doc__
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict(orient="records") == [{"sky_points_check": 1}]
