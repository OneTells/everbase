from asyncpg import Pool, create_pool, Connection as Connection_
from asyncpg.pool import PoolAcquireContext
from asyncpg.transaction import Transaction as Transaction_
from loguru import logger
from pydantic import BaseModel


class Connection:

    def __init__(self, pool_acquire_context: PoolAcquireContext) -> None:
        self.__pool_acquire_context = pool_acquire_context

    async def __aenter__(self) -> Connection_:
        return await self.__pool_acquire_context.__aenter__()

    async def __aexit__(self, *exc) -> None:
        await self.__pool_acquire_context.__aexit__(*exc)


class Transaction:

    def __init__(self, pool_acquire_context: PoolAcquireContext) -> None:
        self.__pool_acquire_context = pool_acquire_context
        self.__transaction: Transaction_ | None = None

    async def __aenter__(self) -> Connection_:
        connection = await self.__pool_acquire_context.__aenter__()

        self.__transaction = connection.transaction()
        await self.__transaction.__aenter__()

        return connection

    async def __aexit__(self, *exc) -> None:
        await self.__transaction.__aexit__(*exc)
        await self.__pool_acquire_context.__aexit__(*exc)


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
        self.__pool = await create_pool(
            self.__settings.dsn,
            min_size=self.__pool_size,
            max_size=self.__pool_size,
            max_inactive_connection_lifetime=120,
            command_timeout=60
        )
        logger.debug(f'База данных {self.__settings.name} подключена')

    async def close(self) -> None:
        await self.__pool.close()
        logger.debug(f'База данных {self.__settings.name} отключена')

    @property
    def pool(self) -> Pool:
        return self.__pool

    def get_transaction(self) -> Transaction:
        return Transaction(self.__pool.acquire())

    def get_connection(self) -> Connection:
        return Connection(self.__pool.acquire())
