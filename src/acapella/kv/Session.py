from typing import List, Union, Optional

import requests
from requests.adapters import HTTPAdapter, DEFAULT_RETRIES
from requests.auth import HTTPBasicAuth
from requests.cookies import cookiejar_from_dict
from urllib3 import Retry

from acapella.kv.BatchManual import BatchManual
from acapella.kv.Entry import Entry
from acapella.kv.PartitionIndex import PartitionIndex
from acapella.kv.Transaction import Transaction
from acapella.kv.TransactionContext import TransactionContext
from acapella.kv.Tree import Tree
from acapella.kv.utils.assertion import check_key, check_clustering, check_limit
from acapella.kv.utils.http import AsyncSession, raise_if_error, entry_url, key_to_str


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
        self._access_token: str = None

    async def login(self, user: Optional[str] = None, password: Optional[str] = None):
        """
        Вход под указанным пользователем.
        :param user: имя пользователя
        :param password: пароль пользователя
        """
        response = await self._session.post('/auth/login', auth=HTTPBasicAuth(user, password))
        raise_if_error(response.status_code)
        body = response.json()
        self._session.set_cookie(cookiejar_from_dict({
            'token': body['token']
        }))

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
            response = await self._session.post('/astorage/v2/tx')
            raise_if_error(response.status_code)
            body = response.json()
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
        entry = Entry(self._session, partition, clustering, 0, None, n, r, w, None)
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
        url = f'{entry_url(partition, clustering)}/version'
        response = await self._session.get(url, params={
            'n': n,
            'r': r,
            'w': w,
        })
        raise_if_error(response.status_code)
        body = response.json()
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
        return Entry(self._session, partition, clustering, 0, None, n, r, w, None)

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

        url = f'/astorage/v2/kv/partition/{key_to_str(partition)}'
        response = await self._session.get(url, params={
            'from': first and key_to_str(first),
            'to': last and key_to_str(last),
            'limit': limit,
            'prefix': prefix and key_to_str(prefix),
            'n': n,
            'r': r,
            'w': w,
        })
        raise_if_error(response.status_code)
        body = response.json()
        return [Entry(self._session, partition, e['key'], e['version'], e['value'], n, r, w, None) for e in body]

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

    def batch_manual(self) -> BatchManual:
        return BatchManual(self._session)

    def partition_index(self, partition: List[str]) -> PartitionIndex:
        return PartitionIndex(self._session, partition)
