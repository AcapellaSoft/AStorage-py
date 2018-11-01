from typing import List, Optional

from acapelladb.Cursor import Cursor
from acapelladb.consts import API_PREFIX
from acapelladb.Transaction import Transaction
from acapelladb.utils.assertion import check_key, check_nrw
from acapelladb.utils.collections import remove_none_values
from acapelladb.utils.http import AsyncSession, key_to_str, raise_if_error


class Tree(object):
    def __init__(self, session: AsyncSession, name: List[str], n: int, r: int, w: int):
        check_key(name)
        check_nrw(n, r, w)
        self._name = name
        self._session = session
        self._n = n
        self._r = r
        self._w = w

    async def get_cursor(self, key: List[str], transaction: Optional[Transaction] = None) -> Cursor:
        """
        Получение значения по указанному ключу в дереве.
        :param key: ключ
        :param transaction если указан, курсор привязывается к транзакции
        :return: Cursor для указанного ключа
        """
        cursor = self.cursor(key, transaction)
        await cursor.get()
        return cursor

    def cursor(self, key: List[str], transaction: Optional[Transaction] = None) -> Cursor:
        """
        Создание Cursor для указанного ключа в дереве. Не выполняет никаких запросов.
        Можно использовать, если нет необходимости знать текущие значение и версию.
        :param key: ключ
        :param transaction если указан, запрос выполняется в транзакции
        :return: Cursor для указанного ключа
        """
        tx_index = transaction.index if transaction is not None else None
        return Cursor(self._session, self._name, key, 0, None, None, self._n, self._r, self._w, tx_index)

    async def range(self,
                    first: Optional[List[str]] = None,
                    last: Optional[List[str]] = None,
                    limit: int = 0,
                    transaction: Optional[Transaction] = None) -> List[Cursor]:
        """
        Возвращает отсортированный список ключей в дереве в указанный пределах.
        :param first: начальный ключ, не включается в ответ; по умолчанию - с первого
        :param last: последий ключ, включается в ответ; по умолчанию - до последнего включительно
        :param limit: максимальное количество ключей в ответе, начиная с первого; по умолчанию - нет ограничений
        :param transaction: если указан, запрос выполняется в транзакции
        :return: список объектов Cursor с данными 
        """
        first = first or []
        last = last or []
        tx_index = transaction.index if transaction is not None else None

        response = await self._session.get(
            f'{API_PREFIX}/v2/dt/{key_to_str(self._name)}/keys',
            params=remove_none_values({
                'from': key_to_str(first),
                'to': key_to_str(last),
                'limit': limit,
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': tx_index,
            })
        )
        raise_if_error(response.status)

        body = await response.json()

        def cursor_init(k, v):
            return Cursor(self._session, self._name, k, 0, v, None, self._n, self._r, self._w, tx_index)

        # TODO: версия в DT
        return [cursor_init(c['key'], c['value']) for c in body]
