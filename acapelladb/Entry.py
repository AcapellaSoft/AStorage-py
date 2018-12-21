from datetime import timedelta
from typing import List, Optional, Awaitable, Callable

import asyncio

from acapelladb.BatchBase import BatchBase
from acapelladb.utils.assertion import check_key, check_nrw
from acapelladb.utils.collections import remove_none_values
from acapelladb.utils.http import AsyncSession, raise_if_error, entry_url


class Entry(object):
    def __init__(self, session: AsyncSession, api_prefix: str, partition: List[str], clustering: List[str],
                 version: int, value: Optional[any], n: int, r: int, w: int, transaction: Optional[int]):
        """
        Создание объекта связанного с указанным ключом. Этот метод предназначен для внутреннего использования.
        """
        check_key(partition)
        check_nrw(n, r, w)
        self._session = session
        self._partition = partition
        self._clustering = clustering
        self._version = version
        self._value = value
        self._n = n
        self._r = r
        self._w = w
        self._transaction = transaction
        self._api_prefix = api_prefix

    async def get(self, watch: bool = False) -> Optional[any]:
        """
        Запрашивает текущее значение с сервера.
        Запоминает новые значение и версию.

        :param watch: если true, то блокирует ключ до конца транзакции
        :return: полученное значение
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        url = entry_url(self._api_prefix, self._partition, self._clustering)
        response = await self._session.get(url, params=remove_none_values({
            'n': self._n,
            'r': self._r,
            'w': self._w,
            'transaction': self._transaction,
            'watch': str(watch),
        }))
        raise_if_error(response.status)
        body = await response.json()
        self._version = int(body['version'])
        self._value = body.get('value')
        return self._value

    async def listen(self, wait_version: Optional[int] = None, timeout: Optional[timedelta] = None) -> Optional[any]:
        """
        Ожидает, пока версия значения не превысит указанную. 
        Если ожидание завершилось успешно, запоминает новые значение и версию.
        
        :param wait_version: ожидаемая версия; если не указана, то используется текущая версия 
        :param timeout: время ожидания; если не указано, то используется значение по умолчанию
        :return: полученное значение
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        if wait_version is None:
            wait_version = self._version
        timeout_seconds = timeout.total_seconds() if timeout is not None else None

        url = entry_url(self._api_prefix, self._partition, self._clustering)
        response = await self._session.get(url, params=remove_none_values({
            'n': self._n,
            'r': self._r,
            'w': self._w,
            'transaction': self._transaction,
            'waitVersion': wait_version,
            'waitTimeout': timeout_seconds,
        }))
        raise_if_error(response.status)

        body = await response.json()
        self._version = int(body['version'])
        self._value = body.get('value')
        return self._value

    def set(self, new_value: Optional[any], reindex: bool = False,
            batch: Optional[BatchBase] = None) -> Awaitable[int]:
        """
        Устанавливает новое значение.
        Запоминает новые значение и версию.        
        
        :param new_value: новое значение
        :param reindex: переиндексировать ключ с новым значением?
        :param batch: батч, в котором нужно выполнить запрос
        :return: новая версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        return single_or_batch(batch, self._set_single, self._set_batch, new_value, reindex)

    async def _set_batch(self, batch: BatchBase, new_value: Optional[any], reindex: bool):
        self._version = await batch.set(self.partition, self.clustering, new_value, reindex, self._n, self._r, self._w)
        self._value = new_value
        return self._version

    async def _set_single(self, new_value: Optional[any], reindex: bool):
        url = entry_url(self._api_prefix, self._partition, self._clustering)
        response = await self._session.put(url, params=remove_none_values({
            'n': self._n,
            'r': self._r,
            'w': self._w,
            'transaction': self._transaction,
            'reindex': str(reindex),
        }), json=new_value)
        raise_if_error(response.status)
        body = await response.json()
        self._version = int(body['version'])
        self._value = new_value
        return self._version

    def cas(self, new_value: Optional[any], old_version: Optional[int] = None, reindex: bool = False,
            batch: Optional[BatchBase] = None) -> Awaitable[int]:
        """
        Устанавливает новое значение при совпадении версий.
        
        :param new_value: новое значение
        :param old_version: старая версия; если не указана, то используется текущая версия
        :param reindex: переиндексировать ключ с новым значением?
        :param batch: батч, в котором нужно выполнить запрос
        :return: новая версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise CasError: когда текущая версия значения не совпала с указанной
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        if old_version is None:
            old_version = self._version
        return single_or_batch(batch, self._cas_single, self._cas_batch, new_value, old_version, reindex)

    async def _cas_batch(self, batch: BatchBase, new_value: Optional[any], old_version: int, reindex: bool):
        self._version = await batch.cas(
            self.partition, self.clustering, new_value, old_version, reindex, self._n, self._r, self._w
        )
        self._value = new_value
        return self._version

    async def _cas_single(self, new_value: Optional[any], old_version: int, reindex: bool):
        url = entry_url(self._api_prefix, self._partition, self._clustering)
        response = await self._session.put(url, params=remove_none_values({
            'n': self._n,
            'r': self._r,
            'w': self._w,
            'transaction': self._transaction,
            'oldVersion': old_version,
            'reindex': str(reindex),
        }), json=new_value)
        raise_if_error(response.status)
        body = await response.json()
        self._version = int(body['version'])
        self._value = new_value
        return self._version

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
    def partition(self) -> List[str]:
        """
        :return: распределительный ключ
        """
        return self._partition

    @property
    def clustering(self) -> List[str]:
        """
        :return: сортируемый ключ
        """
        return self._clustering


def single_or_batch(batch: Optional[BatchBase], fn_single: Callable, fn_batch: Callable, *args) -> Awaitable:
    if batch is None:
        return fn_single(*args)
    else:
        return asyncio.ensure_future(fn_batch(batch, *args))
