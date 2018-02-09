from typing import List, Optional

from acapella.kv.Entry import Entry
from acapella.kv.consts import API_PREFIX
from acapella.kv.utils.http import AsyncSession, raise_if_error


class Transaction(object):
    def __init__(self, session: AsyncSession, index: int):
        """        
        Создание транзакции. Этот метод предназначен для внутреннего использования.
        """
        self._session = session
        self._index = index
        self._completed = False

    async def commit(self):
        """
        Применение транзакции. Применить/откатить транзакцию можно только один раз.
        
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, не найдена 
        :raise TransactionCompletedError: когда транзакция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере         
        """
        if not self._completed:
            response = await self._session.post(f'{API_PREFIX}/v2/tx/{self._index}/commit')
            raise_if_error(response.status)
            self._completed = True

    async def rollback(self):
        """
        Откат транзакции. Применить/откатить транзакцию можно только один раз.
        Если транзакция уже завершена, откат не выполняется, но не бросает искличение.
        
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        if not self._completed:
            response = await self._session.post(f'{API_PREFIX}/v2/tx/{self._index}/rollback')
            raise_if_error(response.status)
            self._completed = True

    async def keep_alive(self):
        """
        Продление жизни транзакции.
        Этот запрос необходим, чтобы определять зависшие транзакции.
        
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, не найдена 
        :raise TransactionCompletedError: когда транзакция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        response = await self._session.post(f'{API_PREFIX}/v2/tx/{self._index}/keep-alive')
        raise_if_error(response.status)

    async def get_entry(self, partition: List[str], clustering: Optional[List[str]] = None,
                        watch: bool = False, n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """        
        Получение значения по указанному ключу в транзакции.
        
        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param watch: если true, то блокирует ключ до конца транзакции
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа с полученным значением
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        entry = self.entry(partition, clustering, n, r, w)
        await entry.get(watch)
        return entry

    def entry(self, partition: List[str], clustering: Optional[List[str]] = None,
              n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """
        Создание Entry для указанного ключа в транзакции. Не выполняет никаких запросов.
        Можно использовать, если нет необходимости знать текущие значение и версию.
        
        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа
        """
        clustering = clustering or []
        return Entry(self._session, partition, clustering, 0, None, n, r, w, self._index)

    @property
    def index(self) -> int:
        """
        :return: индекс транзакции 
        """
        return self._index
