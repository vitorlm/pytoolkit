import datetime
import json
import uuid
from typing import Any, Dict, List


class DuckDBDataValidator:
    def _is_valid_uuid(self, val: Any) -> bool:
        if isinstance(val, uuid.UUID):
            return True
        if isinstance(val, str):
            try:
                uuid.UUID(val)
                return True
            except ValueError:
                return False
        return False

    def _is_valid_json(self, val: Any) -> bool:
        if isinstance(val, (dict, list)):
            return True
        if isinstance(val, str):
            try:
                json.loads(val)
                return True
            except (ValueError, json.JSONDecodeError):
                return False
        return False

    def _is_valid_datetime(self, val: Any) -> bool:
        if isinstance(val, (datetime.datetime, datetime.date)):
            return True
        if isinstance(val, str):
            try:
                datetime.datetime.fromisoformat(val)
                return True
            except ValueError:
                return False
        return False

    def _is_date_only(self, val: Any) -> bool:
        if isinstance(val, datetime.date) and not isinstance(val, datetime.datetime):
            return True
        if isinstance(val, str):
            try:
                dt = datetime.datetime.fromisoformat(val)
                return dt.time() == datetime.time.min
            except ValueError:
                return False
        return False

    def _validate_value(self, value: Any, dtype: str) -> bool:
        if value is None:
            return True
        try:
            dtype = dtype.upper()
            if dtype.startswith("DECIMAL"):
                precision, scale = map(int, dtype.strip("DECIMAL()").split(","))
                if isinstance(value, (int, float)):
                    str_val = f"{abs(value):.{scale}f}".replace(".", "")
                    return len(str_val) <= precision
                return False
            elif dtype == "BOOLEAN":
                return isinstance(value, bool)
            elif dtype in ("FLOAT", "DOUBLE", "REAL"):
                return isinstance(value, (int, float))
            elif dtype in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"):
                if not isinstance(value, int):
                    return False
                if dtype == "TINYINT" and not (-128 <= value <= 127):
                    return False
                if dtype == "SMALLINT" and not (-32768 <= value <= 32767):
                    return False
                if dtype == "INTEGER" and not (-2147483648 <= value <= 2147483647):
                    return False
                if dtype == "BIGINT" and not (-9223372036854775808 <= value <= 9223372036854775807):
                    return False
                return True
            elif dtype == "DATE":
                return self._is_date_only(value)
            elif dtype in ("TIMESTAMP", "TIMESTAMPTZ"):
                return self._is_valid_datetime(value)
            elif dtype == "UUID":
                return self._is_valid_uuid(value)
            elif dtype == "JSON":
                return self._is_valid_json(value)
            elif dtype == "VARCHAR":
                return isinstance(value, str)
            elif dtype.endswith("[]"):
                if not isinstance(value, list):
                    return False
                inner_type = dtype[:-2].strip()
                return all(self._validate_value(item, inner_type) for item in value)
            elif dtype.startswith("STRUCT"):
                return self._is_valid_json(value)
            elif dtype.startswith("MAP"):
                return isinstance(value, dict)
            else:
                return False
        except Exception:
            return False

    def _infer_value_type(self, value: Any) -> str:
        if value is None:
            return "VARCHAR"
        if isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            if -128 <= value <= 127:
                return "TINYINT"
            elif -32768 <= value <= 32767:
                return "SMALLINT"
            elif -2147483648 <= value <= 2147483647:
                return "INTEGER"
            elif -9223372036854775808 <= value <= 9223372036854775807:
                return "BIGINT"
            else:
                return "HUGEINT"
        elif isinstance(value, float):
            return "DOUBLE"
        elif isinstance(value, str):
            if self._is_valid_datetime(value):
                if self._is_date_only(value):
                    return "DATE"
                else:
                    return "TIMESTAMP"
            elif self._is_valid_uuid(value):
                return "UUID"
            elif self._is_valid_json(value):
                return "JSON"
            else:
                return "VARCHAR"
        elif isinstance(value, list):
            return self._infer_list_type(value)
        elif isinstance(value, dict):
            return "JSON"
        else:
            return "VARCHAR"

    def _infer_list_type(self, lst: List[Any]) -> str:
        if not lst:
            return "VARCHAR[]"
        inner_types = {self._infer_value_type(item) for item in lst if item is not None}
        if len(inner_types) == 1:
            inner_type = inner_types.pop()
        else:
            inner_type = "VARCHAR"
        return f"{inner_type}[]"

    def validate_record(self, record: Dict[str, Any], schema: Dict[str, str]) -> bool:
        for col, dtype in schema.items():
            value = record.get(col)
            if not self._validate_value(value, dtype):
                return False
        return True

    def infer_schema(self, records: List[Dict[str, Any]]) -> Dict[str, str]:
        schema: Dict[str, str] = {}
        if not records:
            return schema

        all_keys = {key for record in records for key in record}
        for col in all_keys:
            col_values = [
                record[col] for record in records if col in record and record[col] is not None
            ]
            if not col_values:
                schema[col] = "VARCHAR"
                continue

            types = {type(v) for v in col_values}
            if len(types) > 1:
                schema[col] = "VARCHAR"

            sample_type = types.pop()
            if sample_type == bool:
                schema[col] = "BOOLEAN"
            elif sample_type == int:
                min_val = min(col_values)
                max_val = max(col_values)
                if -128 <= min_val and max_val <= 127:
                    schema[col] = "TINYINT"
                elif -32768 <= min_val and max_val <= 32767:
                    schema[col] = "SMALLINT"
                elif -2147483648 <= min_val and max_val <= 2147483647:
                    schema[col] = "INTEGER"
                elif -9223372036854775808 <= min_val and max_val <= 9223372036854775807:
                    schema[col] = "BIGINT"
                else:
                    schema[col] = "HUGEINT"
            elif sample_type == float:
                schema[col] = "DOUBLE"
            elif sample_type == str:
                if all(self._is_valid_datetime(v) for v in col_values):
                    if all(self._is_date_only(v) for v in col_values):
                        schema[col] = "DATE"
                    else:
                        schema[col] = "TIMESTAMP"
                elif all(self._is_valid_uuid(v) for v in col_values):
                    schema[col] = "UUID"
                elif all(self._is_valid_json(v) for v in col_values):
                    schema[col] = "VARCHAR"
                else:
                    schema[col] = "VARCHAR"
            elif sample_type == dict:
                if all(self._is_valid_json(v) for v in col_values):
                    schema[col] = "VARCHAR"
            elif sample_type == list:
                lists = [
                    record[col]
                    for record in records
                    if col in record and isinstance(record[col], list)
                ]
                inner_values = []
                for lst in lists:
                    inner_values.extend(lst)
                if inner_values:
                    inferred = {
                        self._infer_value_type(item) for item in inner_values if item is not None
                    }
                    inner_type = inferred.pop() if len(inferred) == 1 else "VARCHAR"
                    schema[col] = f"{inner_type}[]"
                else:
                    schema[col] = "VARCHAR[]"
            else:
                schema[col] = "VARCHAR"
        return schema
