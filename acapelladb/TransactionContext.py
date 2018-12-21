from acapelladb.Transaction import Transaction

from acapelladb.utils.http import raise_if_error, AsyncSession


class TransactionContext(object):
    def __init__(self, session: AsyncSession, api_prefix: str):
        """        
        Создание контекста транзакции. Этот метод предназначен для внутреннего использования.
        """
        self._session = session
        self._api_prefix = api_prefix
        self._tx = None  # type: Transaction

    # для типизации
    def __enter__(self) -> Transaction:
        raise RuntimeError()

    async def __aenter__(self) -> Transaction:
        if self._tx is not None:
            raise RuntimeError("This transaction context already in entered state")

        response = await self._session.post(f'{self._api_prefix}/v2/tx')
        raise_if_error(response.status)
        body = await response.json()
        index = int(body['index'])
        tx = Transaction(self._session, index, self._api_prefix)
        self._tx = tx
        return tx

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self._tx.commit()
        else:
            await self._tx.rollback()
        self._tx = None
