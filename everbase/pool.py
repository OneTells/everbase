from typing import Annotated, Any

from asyncpg import Pool, create_pool
from asyncpg.pool import PoolAcquireContext
from pydantic import BaseModel, Field, PostgresDsn, validate_call


class DatabaseSettings(BaseModel):
    user: Annotated[str, Field()]
    password: Annotated[str, Field()]
    host: Annotated[str, Field()]
    name: Annotated[str, Field()]
    port: Annotated[int, Field()]

    @property
    def dsn(self) -> str:
        return f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}'


class DatabasePool:

    @validate_call
    def __init__(
        self,
        settings: DatabaseSettings | PostgresDsn,
        *,
        pool_size: int = 5,
        max_inactive_connection_lifetime: float = 120,
        command_timeout: float = 60,
        **connect_kwargs: Any
    ) -> None:
        self.__dsn = settings.encoded_string() if isinstance(settings, PostgresDsn) else settings.dsn

        self.__pool_size = pool_size
        self.__max_inactive_connection_lifetime = max_inactive_connection_lifetime
        self.__command_timeout = command_timeout
        self.__connect_kwargs = connect_kwargs

        self.__pool: Pool | None = None

    async def connect(self) -> None:
        if self.__pool is not None:
            return

        self.__pool = await create_pool(
            self.__dsn,
            min_size=self.__pool_size,
            max_size=self.__pool_size,
            max_inactive_connection_lifetime=self.__max_inactive_connection_lifetime,
            command_timeout=self.__command_timeout,
            **self.__connect_kwargs
        )

    async def close(self) -> None:
        if self.__pool.is_closing():
            return

        await self.__pool.close()

    @property
    def pool(self) -> Pool:
        return self.__pool

    def get_connection(self) -> PoolAcquireContext:
        return self.__pool.acquire()
