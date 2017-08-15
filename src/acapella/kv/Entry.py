from datetime import timedelta
from typing import List, Optional

from acapella.kv.BatchBase import BatchBase
from acapella.kv.utils.assertion import check_key, check_nrw
from acapella.kv.utils.http import AsyncSession, raise_if_error, entry_url


class Entry(object):
    def __init__(self, session: AsyncSession, partition: List[str], clustering: List[str],
                 version: int, value: Optional[object], n: int, r: int, w: int, transaction: Optional[int]):
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

    async def get(self) -> Optional[object]:
        """
        Запрашивает текущее значение с сервера.
        Запоминает новые значение и версию.
        
        :return: полученное значение
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        url = entry_url(self._partition, self._clustering)
        response = await self._session.get(url, params={
            'n': self._n,
            'r': self._r,
            'w': self._w,
            'transaction': self._transaction,
        })
        raise_if_error(response.status_code)
        body = response.json()
        self._version = int(body['version'])
        self._value = body.get('value')
        return self._value

    async def listen(self, wait_version: Optional[int] = None, timeout: Optional[timedelta] = None) -> Optional[object]:
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

        url = entry_url(self._partition, self._clustering)
        response = await self._session.get(url, params={
            'n': self._n,
            'r': self._r,
            'w': self._w,
            'transaction': self._transaction,
            'waitVersion': wait_version,
            'waitTimeout': timeout_seconds,
        })
        raise_if_error(response.status_code)

        body = response.json()
        self._version = int(body['version'])
        self._value = body.get('value')
        return self._value

    async def set(self, new_value: Optional[object], batch: Optional[BatchBase] = None) -> int:
        """
        Устанавливает новое значение.
        Запоминает новые значение и версию.        
        
        :param new_value: новое значение
        :param batch: батч, в котором нужно выполнить запрос
        :return: новая версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        if batch is None:
            url = entry_url(self._partition, self._clustering)
            response = await self._session.put(url, params={
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': self._transaction,
            }, json=new_value)
            raise_if_error(response.status_code)

            body = response.json()
            self._version = int(body['version'])
            self._value = new_value
            return self._version
        else:
            self._version = await batch.set(self.partition, self.clustering, new_value, self._n, self._r, self._w)
            self._value = new_value
            return self._version

    async def cas(self, new_value: Optional[object], old_version: Optional[int] = None,
                  batch: Optional[BatchBase] = None) -> int:
        """
        Устанавливает новое значение при совпадении версий.
        
        :param new_value: новое значение
        :param old_version: старая версия; если не указана, то используется текущая версия
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

        if batch is None:
            url = entry_url(self._partition, self._clustering)
            response = await self._session.put(url, params={
                'n': self._n,
                'r': self._r,
                'w': self._w,
                'transaction': self._transaction,
                'oldVersion': old_version,
            }, json=new_value)
            raise_if_error(response.status_code)

            body = response.json()
            self._version = int(body['version'])
            self._value = new_value
            return self._version
        else:
            self._version = await batch.cas(self.partition, self.clustering, new_value, old_version,
                                            self._n, self._r, self._w)
            self._value = new_value
            return self._version

    @property
    def value(self) -> Optional[object]:
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
