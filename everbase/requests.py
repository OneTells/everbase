from typing import overload, Callable, Self, Any, override, Union, Sequence, TYPE_CHECKING, Optional, Iterable

from asyncpg import Record, Connection
from pydantic import BaseModel
from sqlalchemy import Update as Update_, Select as Select_, Delete as Delete_
from sqlalchemy.dialects._typing import _OnConflictConstraintT, _OnConflictIndexElementsT, _OnConflictIndexWhereT, \
    _OnConflictSetT, _OnConflictWhereT
from sqlalchemy.dialects.postgresql import Insert as Insert_
from sqlalchemy.sql._typing import _ColumnsClauseArgument as Columns, _ColumnExpressionArgument, _ColumnsClauseArgument, \
    _FromClauseArgument, _DMLColumnKeyMapping, _ColumnExpressionOrStrLabelArgument, _LimitOffsetType, _JoinTargetArgument, \
    _OnClauseArgument, _DMLColumnArgument
from sqlalchemy.sql.base import _NoArg
from sqlalchemy.sql.selectable import _ForUpdateOfArgument
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
    async def fetch_all[T: BaseModel](
        self,
        connection: Connection | DatabasePool,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[Result](
        self,
        connection: Connection | DatabasePool,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all(
        self,
        connection: Connection | DatabasePool,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
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

    if TYPE_CHECKING:
        @override
        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            sort_by_parameter_order: bool = False,
            **__kw: Any
        ) -> Self:
            ...

        @override
        def values(self, *args: Union[_DMLColumnKeyMapping[Any], Sequence[Any]], **kwargs: Any) -> Self:
            ...

        @override
        def on_conflict_do_update(
            self,
            constraint: _OnConflictConstraintT = None,
            index_elements: _OnConflictIndexElementsT = None,
            index_where: _OnConflictIndexWhereT = None,
            set_: _OnConflictSetT = None,
            where: _OnConflictWhereT = None,
        ) -> Self:
            ...

        @override
        def on_conflict_do_nothing(
            self,
            constraint: _OnConflictConstraintT = None,
            index_elements: _OnConflictIndexElementsT = None,
            index_where: _OnConflictIndexWhereT = None,
        ) -> Self:
            ...

        @override
        def return_defaults(
            self,
            *cols: _DMLColumnArgument,
            supplemental_cols: Optional[Iterable[_DMLColumnArgument]] = None,
            sort_by_parameter_order: bool = False,
        ) -> Self:
            ...


class Select(Select_):

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
    ) -> list[Record] | list[Result] | list[T]:
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

    if TYPE_CHECKING:
        @override
        def select_from(self, *froms: _FromClauseArgument) -> Self:
            ...

        @override
        def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
            ...

        @override
        def having(self, *having: _ColumnExpressionArgument[bool] | bool) -> Self:
            ...

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

        @override
        def limit(self, limit: _LimitOffsetType) -> Self:
            ...

        @override
        def offset(self, offset: _LimitOffsetType) -> Self:
            ...

        @override
        def group_by(
            self,
            __first: Union[
                Literal[None, _NoArg.NO_ARG],
                _ColumnExpressionOrStrLabelArgument[Any],
            ] = _NoArg.NO_ARG,
            *clauses: _ColumnExpressionOrStrLabelArgument[Any],
        ) -> Self:
            ...

        @override
        def join(
            self,
            target: _JoinTargetArgument,
            onclause: Optional[_OnClauseArgument] = None,
            *,
            isouter: bool = False,
            full: bool = False,
        ) -> Self:
            ...

        @override
        def outerjoin(
            self,
            target: _JoinTargetArgument,
            onclause: Optional[_OnClauseArgument] = None,
            *,
            full: bool = False,
        ) -> Self:
            ...

        @override
        def join_from(
            self,
            from_: _FromClauseArgument,
            target: _JoinTargetArgument,
            onclause: Optional[_OnClauseArgument] = None,
            *,
            isouter: bool = False,
            full: bool = False,
        ) -> Self:
            ...

        @override
        def outerjoin_from(
            self,
            from_: _FromClauseArgument,
            target: _JoinTargetArgument,
            onclause: Optional[_OnClauseArgument] = None,
            *,
            full: bool = False,
        ) -> Self:
            ...

        @override
        def distinct(self, *expr: _ColumnExpressionArgument[Any]) -> Self:
            ...

        @override
        def with_for_update(
            self,
            *,
            nowait: bool = False,
            read: bool = False,
            of: Optional[_ForUpdateOfArgument] = None,
            skip_locked: bool = False,
            key_share: bool = False,
        ) -> Self:
            ...


class Update(Update_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def fetch_all[T: BaseModel](
        self,
        connection: Connection | DatabasePool,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[Result](
        self,
        connection: Connection | DatabasePool,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all(
        self,
        connection: Connection | DatabasePool,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
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

    if TYPE_CHECKING:
        @override
        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            **__kw: Any
        ) -> Self:
            ...

        @override
        def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
            ...

        @override
        def values(self, *args: Union[_DMLColumnKeyMapping[Any], Sequence[Any]], **kwargs: Any) -> Self:
            ...

        @override
        def return_defaults(
            self,
            *cols: _DMLColumnArgument,
            supplemental_cols: Optional[Iterable[_DMLColumnArgument]] = None,
            sort_by_parameter_order: bool = False,
        ) -> Self:
            ...


class Delete(Delete_):

    async def execute(
        self,
        connection: Connection | DatabasePool
    ) -> None:
        await Database.execute(self, connection=connection)

    @overload
    async def fetch_all[T: BaseModel](
        self,
        connection: Connection | DatabasePool,
        model: type[T]
    ) -> list[T]:
        ...

    @overload
    async def fetch_all[Result](
        self,
        connection: Connection | DatabasePool,
        model: Callable[[Record], Result]
    ) -> list[Result]:
        ...

    @overload
    async def fetch_all(
        self,
        connection: Connection | DatabasePool,
        model: None = None
    ) -> list[Record]:
        ...

    async def fetch_all[T: BaseModel, Result](
        self,
        connection: Connection | DatabasePool,
        model: type[T] | Callable[[Record], Result] | None = None
    ) -> list[Record] | list[Result] | list[T]:
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

    if TYPE_CHECKING:
        @override
        def returning(
            self,
            *cols: _ColumnsClauseArgument[Any],
            **__kw: Any
        ) -> Self:
            ...

        @override
        def where(self, *whereclause: _ColumnExpressionArgument[bool] | bool) -> Self:
            ...

        @override
        def return_defaults(
            self,
            *cols: _DMLColumnArgument,
            supplemental_cols: Optional[Iterable[_DMLColumnArgument]] = None,
            sort_by_parameter_order: bool = False,
        ) -> Self:
            ...
