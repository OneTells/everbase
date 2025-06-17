from asyncpg import Pool, create_pool
from asyncpg.pool import PoolAcquireContext
from pydantic import BaseModel


class DatabaseSettings(BaseModel):
    user: str
    password: str
    host: str
    name: str
    port: int

    @property
    def dsn(self) -> str:
        return f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}'


class DatabasePool:

    def __init__(self, settings: DatabaseSettings, *, pool_size: int = 5) -> None:
        self.__settings = settings
        self.__pool_size = pool_size

        self.__pool: Pool | None = None

    async def connect(self) -> None:
        if self.__pool is not None:
            return

        self.__pool = await create_pool(
            self.__settings.dsn,
            min_size=self.__pool_size,
            max_size=self.__pool_size,
            max_inactive_connection_lifetime=120,
            command_timeout=60
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
