from inspect import isclass
from typing import Any, Callable

from asyncpg import Record
from pydantic import BaseModel
from sqlalchemy.dialects.postgresql import asyncpg
from sqlalchemy.dialects.postgresql.base import PGCompiler
from sqlalchemy.sql.elements import DQLDMLClauseElement as Query


def compile_query_without_params(query: Query) -> str:
    return PGCompiler(asyncpg.dialect(), query, compile_kwargs={"render_postcompile": True}).string


def compile_query(query: Query) -> tuple[str, tuple[Any, ...]]:
    query_compiled = PGCompiler(asyncpg.dialect(), query, compile_kwargs={"render_postcompile": True})

    params_ = query_compiled.params
    params = tuple(params_[name] for name in query_compiled.positiontup)

    return query_compiled.string, params


def deserialize_records[T: BaseModel, Result](
    records: list[Record],
    model: type[T] | Callable[[Record], Result] | None
) -> list[Record] | list[Result] | list[T]:
    if model is None:
        return records

    if isclass(model) and issubclass(model, BaseModel):
        return list(map(lambda record: model(**record), records))

    return list(map(model, records))


def deserialize_record[T: BaseModel, Result](
    record: Record,
    model: type[T] | Callable[[Record], Result] | None
) -> Record | Result | T:
    if model is None:
        return record

    if isclass(model) and issubclass(model, BaseModel):
        return model(**record)

    return model(record)
