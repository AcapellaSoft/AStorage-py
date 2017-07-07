from typing import List, Union

import requests
from requests.adapters import HTTPAdapter, DEFAULT_RETRIES
from urllib3 import Retry

from acapella.kv.Entry import Entry
from acapella.kv.Transaction import Transaction
from acapella.kv.TransactionContext import TransactionContext
from acapella.kv.Tree import Tree
from acapella.kv.utils.http import AsyncSession, raise_if_error, key_to_str


class Session(object):
    def __init__(self, host: str = '127.0.0.1', port: int = 12000, max_retries: Union[Retry, int] = DEFAULT_RETRIES):
        """
        Создание HTTP-сессии для взаимодействия с KV. 
        
        :param host: хост
        :param port: порт
        :param max_retries: стратегия повторных попыток при таймауте или число повторных попыток
        """
        base_url = f'http://{host}:{port}'
        requests_session = requests.Session()
        adapter = HTTPAdapter(max_retries=max_retries)
        requests_session.mount('http://', adapter)
        requests_session.mount('https://', adapter)
        self._session = AsyncSession(session=requests_session, base_url=base_url)

    def transaction(self) -> TransactionContext:
        """
        Создание контекста транзакции для использования в блоке `async with`.
        При выходе из блока происходит автоматическое применение/откат транзакции, 
        в зависимости от наличия исключений. Возможно завершение транзакции вручную,
        тогда автоматическое завершение не произойдёт.
        Примеры использования:
        
        async with session.transaction() as tx:
            entry e = await tx.get(["some", "key"])
            await e.cas("new_value")
            // автоматически вызовется tx.commit()
        
        
        async with session.transaction() as tx:
            entry e = await tx.get(["some", "key"])
            raise RuntimeError() // автоматически вызовется tx.rollback()
            await e.cas("new_value")            
            
        :return: контекст транзакции
        """
        return TransactionContext(self._session)

    async def transaction_manual(self) -> Transaction:
        """
        Создание транзакции в "ручном режиме". Применение/откат транзакции лежит на клиентском коде.
        Следует использовать, только если не удаётся работать с транзакцией через `async with`.
        
        :return: созданная транзакция
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.post('/v2/tx')
        raise_if_error(response.status_code)
        body = response.json()
        index = int(body['index'])
        return Transaction(self._session, index)

    async def get_entry(self, key: List[str], n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """
        Получение значения по указанному ключу вне транзакции.
        
        :param key: ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа с полученным значением
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        entry = Entry(self._session, key, 0, None, n, r, w, None)
        await entry.get()
        return entry

    async def get_version(self, key: List[str], n: int = 3, r: int = 2, w: int = 2) -> int:
        """
        Получение версии указанного ключа вне транзакции.

        :param key: ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.get(f'/v2/kv/keys/{key_to_str(key)}/version', params={
            'n': n,
            'r': r,
            'w': w,
        })
        raise_if_error(response.status_code)
        body = response.json()
        return int(body['version'])

    def entry(self, key: List[str], n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """
        Создание Entry для указанного ключа вне транзакции. Не выполняет никаких запросов.
        Можно использовать, если нет необходимости знать текущие значение и версию.
        
        :param key: ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа
        """
        return Entry(self._session, key, 0, None, n, r, w, None)

    def tree(self, tree: List[str], n: int = 3, r: int = 2, w: int = 2) -> Tree:
        """
        Создание дерева DT.
        :param tree: имя дерева
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Tree
        """
        return Tree(self._session, tree, n, r, w)
