from typing import Any, Callable, Iterable, Literal, overload, Sequence

from asyncpg import Record
from asyncpg.cursor import CursorFactory
from asyncpg.pool import PoolAcquireContext as PoolAcquireContext_, PoolConnectionProxy
from asyncpg.prepared_stmt import PreparedStatement
from asyncpg.transaction import Transaction
from pydantic import BaseModel
from sqlalchemy.sql.elements import DQLDMLClauseElement as Query

from everbase.utils import compile_query, compile_query_without_params, deserialize_record, deserialize_records


class Connection:

    def __init__(self, connection: PoolConnectionProxy) -> None:
        self._connection = connection

    def transaction(
        self,
        *,
        isolation: Literal['read_committed', 'read_uncommitted', 'serializable', 'repeatable_read'] | None = None,
        readonly: bool = False,
        deferrable: bool = False
    ) -> Transaction:
        return self._connection.transaction(isolation=isolation, readonly=readonly, deferrable=deferrable)

    def cursor(
        self,
        query: Query,
        *,
        prefetch: int | None = None,
        timeout: float | None = None,
        record_class: type[Record] | None = None
    ) -> CursorFactory:
        query_str, params = compile_query(query)
        return self._connection.cursor(query_str, *params, prefetch=prefetch, timeout=timeout, record_class=record_class)

    async def execute(
        self,
        query: Query,
        *,
        timeout: float | None = None
    ) -> str:
        query_str, params = compile_query(query)
        return await self._connection.execute(query_str, *params, timeout=timeout)

    async def execute_many(
        self,
        query: Query,
        args: Iterable[Sequence[Any]],
        *,
        timeout: float | None = None,
    ) -> None:
        query_str = compile_query_without_params(query)
        return await self._connection.executemany(query_str, args, timeout=timeout)

    @overload
    async def fetch(
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch[T: BaseModel](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch[Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch[T: BaseModel, Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
        query_str, params = compile_query(query)
        response = await self._connection.fetch(query_str, *params, timeout=timeout, record_class=record_class)
        return deserialize_records(response, model)

    async def fetch_val(
        self,
        query: Query,
        *,
        column: int = 0,
        timeout: float | None = None
    ) -> Any:
        query_str, params = compile_query(query)
        return await self._connection.fetchval(query_str, *params, column=column, timeout=timeout)

    @overload
    async def fetch_row[TRecord: Record](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[TRecord] | None = None,
        model: None = None
    ) -> list[TRecord]:
        ...

    @overload
    async def fetch_row[T: BaseModel](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_row[Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_row[T: BaseModel, Result](
        self,
        query: Query,
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> Record | Result | T | None:
        query_str, params = compile_query(query)
        response = await self._connection.fetchrow(query_str, *params, timeout=timeout, record_class=record_class)
        return deserialize_record(response, model)

    @overload
    async def fetch_many[TRecord: Record](
        self,
        query: Query,
        args: Iterable[Sequence[Any]],
        *,
        timeout: float | None = None,
        record_class: type[TRecord] | None = None,
        model: None = None
    ) -> list[TRecord]:
        ...

    @overload
    async def fetch_many[T: BaseModel](
        self,
        query: Query,
        args: Iterable[Sequence[Any]],
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_many[Result](
        self,
        query: Query,
        args: Iterable[Sequence[Any]],
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_many[T: BaseModel, Result](
        self,
        query: Query,
        args: Iterable[Sequence[Any]],
        *,
        timeout: float | None = None,
        record_class: type[Record] | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
        query_str = compile_query_without_params(query)
        response = await self._connection.fetchmany(query_str, args, timeout=timeout, record_class=record_class)
        return deserialize_records(response, model)

    async def prepare(
        self,
        query: Query,
        *,
        name: str | None = None,
        timeout: float | None = None,
        record_class: type[Record] | None = None
    ) -> PreparedStatement:
        query_str = compile_query_without_params(query)
        return await self._connection.prepare(query_str, name=name, timeout=timeout, record_class=record_class)


class PoolAcquireContext:

    def __init__(self, context: PoolAcquireContext_) -> None:
        self._context = context

    async def __aenter__(self) -> Connection:
        return Connection(await self._context.__aenter__())

    async def __aexit__(self, *exc) -> None:
        await self._context.__aexit__(*exc)
