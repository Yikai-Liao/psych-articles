from typing import Any, get_args, get_origin, Union
from pydantic import BaseModel

import polars as pl

def _is_union(annotation) -> bool:
    origin = get_origin(annotation)
    return origin is Union or str(origin).endswith("Union")

def _annotation_to_polars_dtype(annotation) -> pl.DataType:
    origin = get_origin(annotation)
    if origin is list:
        (item_type,) = get_args(annotation)
        return pl.List(_annotation_to_polars_dtype(item_type))
    if origin is dict:
        return pl.Object
    if _is_union(annotation):
        args = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(args) == 1:
            return _annotation_to_polars_dtype(args[0])
        if str in args:
            return pl.Utf8
        if float in args and int in args:
            return pl.Float64
        return _annotation_to_polars_dtype(args[0])
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        fields = {
            name: _annotation_to_polars_dtype(field.annotation)
            for name, field in annotation.model_fields.items()
        }
        return pl.Struct(fields)
    if annotation is str:
        return pl.Utf8
    if annotation is int:
        return pl.Int64
    if annotation is float:
        return pl.Float64
    if annotation is bool:
        return pl.Boolean
    if annotation in (Any, object):
        return pl.Utf8
    if annotation is None:
        return pl.Null
    return pl.Utf8

def pl_schema_from_pydantic(model_cls: type[BaseModel]) -> dict[str, pl.DataType]:
    return {
        name: _annotation_to_polars_dtype(field.annotation)
        for name, field in model_cls.model_fields.items()
    }

def pl_df_from_pydantic_list(
    data: list[BaseModel],
) -> pl.DataFrame:
    model_cls = type(data[0])
    schema = pl_schema_from_pydantic(model_cls)
    records = []
    for item in data:
        assert isinstance(item, model_cls), f"All items must be of the same Pydantic model class, finding: {type(item)} vs {model_cls}"
        records.append(item.model_dump())
    return pl.DataFrame(records, schema=schema)

__all__ = ["pl_schema_from_pydantic", "pl_df_from_pydantic_list"]
