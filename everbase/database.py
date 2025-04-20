from inspect import isclass
from typing import Callable as Call, overload, Callable

from asyncpg import Record, Connection
from pydantic import BaseModel
from sqlalchemy import Update as Update_, Select as Select_, Delete as Delete_
from sqlalchemy.dialects.postgresql import Insert as Insert_
from sqlalchemy.sql.dml import ReturningInsert

from everbase.compiler import compile_query
from everbase.pool import DatabasePool

type Query = Select_ | Update_ | Insert_ | Delete_ | ReturningInsert


class Database[T: BaseModel, Result]:

    @classmethod
    @overload
    async def fetch(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: type[T]
    ) -> list[T]:
        ...

    @classmethod
    @overload
    async def fetch(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @classmethod
    @overload
    async def fetch(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: None = None
    ) -> list[Record]:
        ...

    @classmethod
    async def fetch(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: type[T] | Call[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        if isinstance(connection, DatabasePool):
            connection = connection.pool

        result: list[Record] = await connection.fetch(compile_query(query))

        if model is None:
            return result

        if isclass(model) and issubclass(model, BaseModel):
            return list(map(lambda record: model(**record), result))

        return list(map(model, result))

    @classmethod
    @overload
    async def fetch_one(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: type[T]
    ) -> T | None:
        ...

    @classmethod
    @overload
    async def fetch_one(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: Callable[[Record], Result]
    ) -> Result | None:
        ...

    @classmethod
    @overload
    async def fetch_one(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: None = None
    ) -> Record | None:
        ...

    @classmethod
    async def fetch_one(
        cls,
        query: Query,
        connection: Connection | DatabasePool,
        *,
        model: type[T] | Call[[Record], Result] | None = None
    ) -> Record | Result | T | None:
        if isinstance(connection, DatabasePool):
            connection = connection.pool

        result: list[Record] = await connection.fetch(compiled_query := compile_query(query))

        if len(result) == 0:
            return None

        if len(result) > 1:
            raise ValueError(f'Запрос вернул несколько записей. Запрос: {compiled_query}. Записи: {result}')

        if model is None:
            return result[0]

        if isclass(model) and issubclass(model, BaseModel):
            return model(**result[0])

        return model(result[0])

    @classmethod
    async def execute(
        cls,
        query: Query,
        connection: Connection | DatabasePool
    ) -> None:
        if isinstance(connection, DatabasePool):
            connection = connection.pool

        await connection.execute(compile_query(query))
