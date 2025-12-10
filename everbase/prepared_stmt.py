from typing import Any, Callable, Iterable, overload

from asyncpg.prepared_stmt import PreparedStatement as PreparedStatement_
from asyncpg.protocol.record import Record
from pydantic import BaseModel

from everbase.utils import deserialize_record, deserialize_records


class PreparedStatement:

    def __init__(self, prepared_statement: PreparedStatement_) -> None:
        self._prepared_statement = prepared_statement

    @property
    def prepared_statement(self) -> PreparedStatement_:
        return self._prepared_statement

    async def explain(self, args: Iterable[Any], *, analyze: bool = False) -> Any:
        return await self._prepared_statement.explain(*args, analyze=analyze)

    @overload
    async def fetch(
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch[T: BaseModel](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch[Result](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch[T: BaseModel, Result](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
        response = await self._prepared_statement.fetch(*args, timeout=timeout)
        return deserialize_records(response, model)

    @overload
    async def fetch_val(
        self,
        args: Iterable[Any],
        *,
        column: int = 0,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch_val[T: BaseModel](
        self,
        args: Iterable[Any],
        *,
        column: int = 0,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_val[Result](
        self,
        args: Iterable[Any],
        *,
        column: int = 0,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_val[Result](
        self,
        args: Iterable[Any],
        *,
        column: int = 0,
        timeout: float | None = None,
        model: Callable[[Any], Result] | None = None
    ) -> Result | Any:
        response = await self._prepared_statement.fetchval(*args, column=column, timeout=timeout)
        return response if model is None else model(response)

    @overload
    async def fetch_row(
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch_row[T: BaseModel](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_row[Result](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_row[T: BaseModel, Result](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> Record | Result | T | None:
        response = await self._prepared_statement.fetchrow(*args, timeout=timeout)
        return deserialize_record(response, model)

    @overload
    async def fetch_many(
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: None = None
    ) -> list[Record]:
        ...

    @overload
    async def fetch_many[T: BaseModel](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_many[Result](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    async def fetch_many[T: BaseModel, Result](
        self,
        args: Iterable[Any],
        *,
        timeout: float | None = None,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
        response = await self._prepared_statement.fetchmany(args, timeout=timeout)  # type: ignore
        return deserialize_records(response, model)

    async def execute_many(self, args: Iterable[Any], *, timeout: float | None = None) -> None:
        return await self._prepared_statement.executemany(args, timeout=timeout)
