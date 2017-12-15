from acapella.kv.Transaction import Transaction

from acapella.kv.utils.http import raise_if_error, AsyncSession


class TransactionContext(object):
    def __init__(self, session: AsyncSession):
        """        
        Создание контекста транзакции. Этот метод предназначен для внутреннего использования.
        """
        self._session = session
        self._tx = None  # type: Transaction

    # для типизации
    def __enter__(self) -> Transaction:
        raise RuntimeError()

    async def __aenter__(self) -> Transaction:
        if self._tx is not None:
            raise RuntimeError("This transaction context already in entered state")

        response = await self._session.post('/astorage/v2/tx')
        raise_if_error(response.status)
        body = await response.json()
        index = int(body['index'])
        tx = Transaction(self._session, index)
        self._tx = tx
        return tx

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self._tx.commit()
        else:
            await self._tx.rollback()
        self._tx = None
