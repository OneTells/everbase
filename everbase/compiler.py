from sqlalchemy import Update, Select, Delete
from sqlalchemy.dialects.postgresql import Insert, dialect
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.sql.dml import ReturningInsert

from everbase.base import Base


def compile_query(query: Insert | ReturningInsert | Select | Update | Delete) -> str:
    return query.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}).string


def compile_table(table: type[Base]) -> str:
    return CreateTable(Base.metadata.tables[table.__tablename__]).compile(dialect=dialect()).string
