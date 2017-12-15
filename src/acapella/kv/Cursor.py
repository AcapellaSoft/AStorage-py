from typing import List, Optional

from acapella.kv.utils.collections import remove_none_values
from acapella.kv.utils.http import AsyncSession, key_to_str, raise_if_error
from acapella.kv.utils.assertion import check_key, check_nrw


class Cursor(object):
    def __init__(self, session: AsyncSession, tree: List[str], key: List[str], version: int, value: Optional[any],
                 node: Optional[str], n: int, r: int, w: int, transaction: Optional[int]):
        check_key(tree)
        check_key(key)
        check_nrw(n, r, w)
        self._tree = tree
        self._session = session
        self._key = key
        self._version = version
        self._value = value
        self._node = node
        self._n = n
        self._r = r
        self._w = w
        self._transaction = transaction

    async def get(self) -> Optional[any]:
        """
        Запрашивает текущее значение с сервера.
        Запоминает новые значение и версию.

        :return: полученное значение
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise TreeNotFoundError: когда не найдено дерево
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.get(
            f'/astorage/v2/dt/{key_to_str(self._tree)}/keys/{key_to_str(self._key)}',
            params=remove_none_values({
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': self._transaction,
            })
        )
        raise_if_error(response.status)

        body = await response.json()
        # TODO: версия в DT
        # self._version = int(body['version'])
        self._value = body.get('value')
        return self._value

    async def set(self, new_value: Optional[any]) -> int:
        """
        Устанавливает новое значение.
        Запоминает новые значение и версию.

        :param new_value: новое значение
        :return: новая версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise TreeNotFoundError: когда не найдено дерево
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.put(
            f'/astorage/v2/dt/{key_to_str(self._tree)}/keys/{key_to_str(self._key)}',
            params=remove_none_values({
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': self._transaction,
            }),
            json=new_value
        )
        raise_if_error(response.status)

        # TODO: версия в DT
        self._value = new_value
        # body = response.json()
        # self._version = int(body['version'])
        return self._version

    async def next(self) -> Optional['Cursor']:
        """
        Поиск следующего за текущим ключа.
        :return: Cursor со следующим ключом, None - если ключ не найден.
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise TreeNotFoundError: когда не найдено дерево
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.get(
            f'/astorage/v2/dt/{key_to_str(self._tree)}/keys/{key_to_str(self._key)}/next',
            params=remove_none_values({
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': self._transaction,
                'node': self._node,
            })
        )
        if response.status == 404:
            return None
        raise_if_error(response.status)

        body = await response.json()
        return Cursor(self._session, self._tree, body['key'], 0, body.get('value'), body.get('node'),
                      self._n, self._r, self._w, self._transaction)

    async def prev(self) -> Optional['Cursor']:
        """
        Поиск предыдущего перед текущим ключа.
        :return: Cursor с предыдущим ключом, None - если ключ не найден.
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise TreeNotFoundError: когда не найдено дерево
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.get(
            f'/astorage/v2/dt/{key_to_str(self._tree)}/keys/{key_to_str(self._key)}/prev',
            params=remove_none_values({
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': self._transaction,
                'node': self._node,
            })
        )
        if response.status == 404:
            return None
        raise_if_error(response.status)

        body = await response.json()
        return Cursor(self._session, self._tree, body['key'], 0, body.get('value'), body.get('node'),
                      self._n, self._r, self._w, self._transaction)

    @property
    def value(self) -> Optional[any]:
        """
        :return: значение 
        """
        return self._value

    @property
    def version(self) -> int:
        """
        :return: версия 
        """
        return self._version

    @property
    def key(self) -> List[str]:
        """
        :return: ключ 
        """
        return self._key
