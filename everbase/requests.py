from typing import overload, Callable, Self, Any, override, Union, Sequence, TYPE_CHECKING

from asyncpg import Record, Connection
from pydantic import BaseModel
from sqlalchemy import Update as Update_, Select as Select_, Delete as Delete_
from sqlalchemy.dialects.postgresql import Insert as Insert_
from sqlalchemy.sql._typing import _ColumnsClauseArgument as Columns, _ColumnExpressionArgument, _ColumnsClauseArgument, \
    _FromClauseArgument, _DMLColumnKeyMapping, _ColumnExpressionOrStrLabelArgument
from sqlalchemy.sql.base import _NoArg
from typing_extensions import Literal

from everbase.database import Database
from everbase.pool import DatabasePool


class Insert(Insert_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch_all(self, model=model, connection=connection)

    @override
    def returning(self, *cols: _ColumnsClauseArgument[Any], sort_by_parameter_order: bool = False, **__kw: Any) -> Self:
        return super().returning(*cols, **__kw)

    @override
    def values(self, *args: Union[_DMLColumnKeyMapping[Any], Sequence[Any]], **kwargs: Any) -> Self:
        return super().values(*args, **kwargs)


class Select[TP: Columns[Any]](Select_[tuple[TP]]):

    def __init__(self, *entities: TP):
        super().__init__(*entities)

    @overload
    async def fetch_all[T: BaseModel](
        self,
        connection: Connection | DatabasePool,
        *,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[Result](
        self,
        connection: Connection | DatabasePool,
        *,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all(
        self,
        connection: Connection | DatabasePool,
        *,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        *,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch_all(self, model=model, connection=connection)

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

    @override
    def select_from(self, *froms: _FromClauseArgument) -> Self:
        return super().select_from(*froms)

    @override
    def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
        return super().where(*whereclause)

    @override
    def having(self, *having: _ColumnExpressionArgument[bool] | bool) -> Self:
        return super().having(*having)

    if TYPE_CHECKING:
        @override
        def order_by(
            self,
            __first: Union[
                Literal[None, _NoArg.NO_ARG],
                _ColumnExpressionOrStrLabelArgument[Any]
            ] = _NoArg.NO_ARG,
            *clauses: _ColumnExpressionOrStrLabelArgument[Any]
        ) -> Self:
            ...


class Update(Update_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch_all(self, model=model, connection=connection)

    @override
    def returning(self, *cols: _ColumnsClauseArgument[Any], **__kw: Any) -> Self:
        return super().returning(*cols, **__kw)

    @override
    def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
        return super().where(*whereclause)

    @override
    def values(self, *args: Union[_DMLColumnKeyMapping[Any], Sequence[Any]], **kwargs: Any) -> Self:
        return super().values(*args, **kwargs)


class Delete(Delete_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record | Result | T]:
        return await Database.fetch_all(self, model=model, connection=connection)

    @override
    def returning(self, *cols: _ColumnsClauseArgument[Any], **__kw: Any) -> Self:
        return super().returning(*cols, **__kw)

    @override
    def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
        return super().where(*whereclause)
