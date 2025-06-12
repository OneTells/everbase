from typing import overload, Callable, override, Self, Any

from asyncpg import Record, Connection
from pydantic import BaseModel
from sqlalchemy import Update as Update_, Select as Select_, Delete as Delete_
from sqlalchemy.dialects.postgresql import Insert as Insert_
from sqlalchemy.sql._typing import _ColumnsClauseArgument as Columns, _ColumnExpressionArgument

from everbase.database import Database
from everbase.pool import DatabasePool


class Insert(Insert_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: None = None
    ) -> list[Record]:
        ...

    @override
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch(super().returning(*cols), model=model, connection=connection)


class Select[TP: Columns[Any]](Select_[tuple[TP]]):

    def __init__(self, *entities: TP):
        super().__init__(*entities)

    @overload
    async def fetch[T: BaseModel](
        self,
        connection: Connection | DatabasePool,
        *,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch[Result](
        self,
        connection: Connection | DatabasePool,
        *,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch(
        self,
        connection: Connection | DatabasePool,
        *,
        model: None = None
    ) -> list[Record]:
        ...

    @override
    async def fetch[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch(self, model=model, connection=connection)

    @overload
    async def fetch_one[T: BaseModel](
        self,
        connection: Connection | DatabasePool,
        *,
        model: type[T]
    ) -> T | None:
        ...

    @overload
    async def fetch_one[Result](
        self,
        connection: Connection | DatabasePool,
        *,
        model: Callable[[Record], Result]
    ) -> Result | None:
        ...

    @overload
    async def fetch_one(
        self,
        connection: Connection | DatabasePool,
        *,
        model: None = None
    ) -> Record | None:
        ...

    async def fetch_one[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> Record | Result | T | None:
        return await Database.fetch_one(self, model=model, connection=connection)

    def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
        return super().where(*whereclause)

    def having(self, *having: _ColumnExpressionArgument[bool] | bool) -> Self:
        return super().having(*having)


class Update(Update_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: None = None
    ) -> list[Record]:
        ...

    @override
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch(super().returning(*cols), model=model, connection=connection)


class Delete(Delete_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: None = None
    ) -> list[Record]:
        ...

    @override
    async def returning[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *cols: Columns,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch(super().returning(*cols), model=model, connection=connection)
