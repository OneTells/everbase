from typing import Any, Callable, Iterable, Literal, overload

from asyncpg import Record
from asyncpg.pool import PoolConnectionProxy
from asyncpg.transaction import Transaction
from pydantic import BaseModel
from sqlalchemy.sql.elements import DQLDMLClauseElement as Query

from everbase.prepared_stmt import PreparedStatementWrapper
from everbase.utils import compile_query, compile_query_without_params, deserialize_record, deserialize_records


class ConnectionWrapper:

    def __init__(self, connection: PoolConnectionProxy) -> None:
        self._connection: PoolConnectionProxy = connection

    @property
    def value(self) -> PoolConnectionProxy:
        return self._connection

    def transaction(
        self,
        *,
        isolation: Literal['read_committed', 'read_uncommitted', 'serializable', 'repeatable_read'] | None = None,
        readonly: bool = False,
        deferrable: bool = False
    ) -> Transaction:
        return self._connection.transaction(isolation=isolation, readonly=readonly, deferrable=deferrable)

    async def execute(
        self,
        query: Query,
        *,
        timeout: float | None = None,
    ) -> str:
        query_str, params = compile_query(query)
        return await self._connection.execute(query_str, *params, timeout=timeout)

    async def execute_many(
        self,
        query: Query,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
    ) -> None:
        query_str = compile_query_without_params(query)
        return await self._connection.executemany(query_str, args, timeout=timeout)

    async def prepare(
        self,
        query: Query,
        *,
        name: str | None = None,
        timeout: float | None = None
    ) -> PreparedStatementWrapper:
        query_str = compile_query_without_params(query)
        prepared_statement = await self._connection.prepare(query_str, name=name, timeout=timeout)
        return PreparedStatementWrapper(prepared_statement)

    @overload
    async def fetch[T: BaseModel](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch[Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch(
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch[T: BaseModel, Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[T] | list[Result] | list[Record]:
        query_str, params = compile_query(query)
        response = await self._connection.fetch(query_str, *params, timeout=timeout)
        return deserialize_records(response, model)

    @overload
    async def fetch_val(
        self,
        query: Query,
        *,
        column: int = 0,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch_val[T: BaseModel](
        self,
        query: Query,
        *,
        column: int = 0,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_val[Result](
        self,
        query: Query,
        *,
        column: int = 0,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_val[Result](
        self,
        query: Query,
        *,
        column: int = 0,
        timeout: float | None = None,
        model: Callable[[Any], Result] | None = None
    ) -> Result | Any:
        query_str, params = compile_query(query)
        response = await self._connection.fetchval(query_str, *params, column=column, timeout=timeout)
        return response if model is None else model(response)

    @overload
    async def fetch_row(
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch_row[T: BaseModel](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_row[Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_row[T: BaseModel, Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> Record | Result | T | None:
        query_str, params = compile_query(query)
        response = await self._connection.fetchrow(query_str, *params, timeout=timeout)
        return deserialize_record(response, model)

    @overload
    async def fetch_many(
        self,
        query: Query,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch_many[T: BaseModel](
        self,
        query: Query,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_many[Result](
        self,
        query: Query,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_many[T: BaseModel, Result](
        self,
        query: Query,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
        query_str = compile_query_without_params(query)
        response = await self._connection.fetchmany(query_str, args, timeout=timeout)
        return deserialize_records(response, model)
