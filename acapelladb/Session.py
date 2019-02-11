import base64
from typing import List, Union, Optional

from aiohttp import BasicAuth
from requests.adapters import DEFAULT_RETRIES
from urllib3 import Retry

from acapelladb.BatchManual import BatchManual
from acapelladb.Entry import Entry
from acapelladb.PartitionIndex import PartitionIndex
from acapelladb.Transaction import Transaction
from acapelladb.TransactionContext import TransactionContext
from acapelladb.Tree import Tree
from acapelladb.utils.assertion import check_key, check_clustering, check_limit
from acapelladb.utils.collections import remove_none_values
from acapelladb.utils.http import AsyncSession, raise_if_error, entry_url, key_to_str


class Session(object):
    def __init__(self, host: str = '127.0.0.1', port: int = 12000,
                 api_prefix: str = '/acapelladb'):
        """
        Создание HTTP-сессии для взаимодействия с KV. 
        
        :param host: хост
        :param port: порт
        """
        base_url = f'http://{host}:{port}'
        self._session = AsyncSession(base_url=base_url)
        self._api_prefix = api_prefix

    @property
    def api_prefix(self) -> str:
        return self._api_prefix

    @staticmethod
    def _format_basic_auth(user: str, password: str) -> str:
        user_and_password = user + ':' + password
        return base64.standard_b64encode(user_and_password.encode()).decode()

    async def login(self, user: Optional[str] = None, password: Optional[str] = None):
        """
        Вход под указанным пользователем.
        :param user: имя пользователя
        :param password: пароль пользователя
        """
        response = await self._session.post(
            '/auth/login',
            data={
                'enableCookies': 'true',
                'invalidateOld': 'false',
                'loginAndPassword': self._format_basic_auth(user, password)
            }
        )
        raise_if_error(response.status)
        body = await response.json()
        token = body['token']
        self._session.set_auth(BasicAuth(user, token))

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
        return TransactionContext(self._session, self._api_prefix)

    async def transaction_manual(self, index: Optional[int] = None) -> Transaction:
        """
        Создание объекта транзакции в "ручном режиме". Применение/откат транзакции лежит на клиентском коде.
        Следует использовать, только если не удаётся работать с транзакцией через `async with`.

        :param index: если указан, то транзакция не создаётся, а просто оборачивается в объект Transaction
        :return: созданная транзакция
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        if index is None:
            response = await self._session.post(f'{self.api_prefix}/v2/tx')
            raise_if_error(response.status)
            body = await response.json()
            index = int(body['index'])
        return Transaction(self._session, index)

    async def get_entry(self, partition: List[str], clustering: Optional[List[str]] = None,
                        n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """
        Получение значения по указанному ключу вне транзакции.
        
        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа с полученным значением
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        clustering = clustering or []
        entry = Entry(self._session, self._api_prefix, partition, clustering, 0, None, n, r, w, None)
        await entry.get()
        return entry

    async def get_version(self, partition: List[str], clustering: Optional[List[str]] = None,
                          n: int = 3, r: int = 2, w: int = 2) -> int:
        """
        Получение версии указанного ключа вне транзакции.

        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        clustering = clustering or []
        url = f'{entry_url(self._api_prefix, partition, clustering)}/version'
        response = await self._session.get(url, params={
            'n': n,
            'r': r,
            'w': w,
        })
        raise_if_error(response.status)
        body = await response.json()
        return int(body['version'])

    def entry(self, partition: List[str], clustering: Optional[List[str]] = None,
              n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """
        Создание Entry для указанного ключа вне транзакции. Не выполняет никаких запросов.
        Можно использовать, если нет необходимости знать текущие значение и версию.
        
        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа
        """
        clustering = clustering or []
        return Entry(self._session, self._api_prefix, partition, clustering, 0, None, n, r, w, None)

    async def range(self,
                    partition: List[str],
                    first: Optional[List[str]] = None,
                    last: Optional[List[str]] = None,
                    limit: Optional[int] = None,
                    prefix: Optional[List[str]] = None,
                    n: int = 3,
                    r: int = 2,
                    w: int = 2) -> List[Entry]:
        """
        Возвращает отсортированный список ключей в дереве в указанный пределах.
        :param partition: распределительный ключ
        :param first: начальный ключ, не включается в ответ; по умолчанию - с первого
        :param last: последий ключ, включается в ответ; по умолчанию - до последнего включительно
        :param limit: максимальное количество ключей в ответе, начиная с первого; по умолчанию - нет ограничений
        :param prefix: префикс, к которому должны принадлежать все ключи в выборке
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: список объектов Entry с данными
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        check_key(partition)
        check_clustering(first)
        check_clustering(last)
        check_limit(limit)
        check_clustering(prefix)

        url = f'{self.api_prefix}/v2/kv/partition/{key_to_str(partition)}'
        response = await self._session.get(url, params=remove_none_values({
            'from': first and key_to_str(first),
            'to': last and key_to_str(last),
            'limit': limit,
            'prefix': prefix and key_to_str(prefix),
            'n': n,
            'r': r,
            'w': w,
        }))
        raise_if_error(response.status)
        body = await response.json()
        return [Entry(self._session, self._api_prefix, partition, e['key'], e['version'], e.get('value'), n, r, w, None) for e in body]

    def tree(self, tree: List[str], n: int = 3, r: int = 2, w: int = 2) -> Tree:
        """
        Создание дерева DT.
        :param tree: имя дерева
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Tree
        """
        return Tree(self._session, tree, n, r, w, self._api_prefix)

    def batch_manual(self) -> BatchManual:
        return BatchManual(self._session, self._api_prefix)

    def partition_index(self, partition: List[str]) -> PartitionIndex:
        return PartitionIndex(self._session, partition, self._api_prefix)
