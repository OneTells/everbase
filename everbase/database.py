from __future__ import annotations

from typing import Any, Generator, Self

from asyncpg import create_pool, Record
from asyncpg.pool import Pool as Pool_, PoolAcquireContext as PoolAcquireContext_

from everbase.connection import ConnectionWrapper


class Database:

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 5,
        max_size: int = 5,
        max_queries: int = 50000,
        max_inactive_connection_lifetime: float = 120,
        command_timeout: float = 60,
        record_class: type[Record] = Record,
        **connect_kwargs
    ) -> None:
        self._dsn = dsn
        self._kwargs = {
            'min_size': min_size,
            'max_size': max_size,
            'max_queries': max_queries,
            'max_inactive_connection_lifetime': max_inactive_connection_lifetime,
            'command_timeout': command_timeout,
            'record_class': record_class,
            **connect_kwargs
        }

        self._pool: Pool_ | None = None

    @property
    def pool(self) -> Pool_:
        if self._pool is None:
            raise ValueError('Pool is not connected')

        return self._pool

    def acquire(self, *, timeout: float | None = None) -> PoolAcquireContextWrapper:
        if self._pool is None:
            raise ValueError('Pool is not connected')

        pool_acquire_context = self._pool.acquire(timeout=timeout)
        return PoolAcquireContextWrapper(pool_acquire_context)

    async def release(self, connection: ConnectionWrapper, *, timeout: float | None = None) -> None:
        await self._pool.release(connection.value, timeout=timeout)

    async def connect(self) -> None:
        if self._pool is not None:
            return

        self._pool = await create_pool(self._dsn, **self._kwargs)

    async def close(self) -> None:
        if self._pool is None:
            return

        await self._pool.close()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


class PoolAcquireContextWrapper:

    def __init__(self, pool_acquire_context: PoolAcquireContext_) -> None:
        self._pool_acquire_context = pool_acquire_context

    async def __aenter__(self) -> ConnectionWrapper:
        connection = await self._pool_acquire_context.__aenter__()
        return ConnectionWrapper(connection)

    async def __aexit__(self, *exc: Any) -> None:
        await self._pool_acquire_context.__aexit__(*exc)

    def __await__(self) -> Generator[Any, None, ConnectionWrapper]:
        connection = yield from self._pool_acquire_context.__await__()
        return ConnectionWrapper(connection)
