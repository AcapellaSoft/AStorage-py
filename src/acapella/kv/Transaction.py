from typing import List

from acapella.kv.Entry import Entry
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
            response = await self._session.post(f'/v2/tx/{self._index}/commit')
            raise_if_error(response.status_code)
            self._completed = True

    async def rollback(self):
        """
        Откат транзакции. Применить/откатить транзакцию можно только один раз.
        Если транзакция уже завершена, откат не выполняется, но не бросает искличение.
        
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        if not self._completed:
            response = await self._session.post(f'/v2/tx/{self._index}/rollback')
            raise_if_error(response.status_code)
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
        response = await self._session.post(f'/v2/tx/{self._index}/keep-alive')
        raise_if_error(response.status_code)

    async def get_entry(self, key: List[str], n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """        
        Получение значения по указанному ключу в транзакции.
        
        :param key: ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа с полученным значением
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise TransactionNotFoundError: когда транзакция, в которой выполняется операция, не найдена 
        :raise TransactionCompletedError: когда транзакция, в которой выполняется операция, уже завершена
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        entry = self.entry(key, n, r, w)
        await entry.get()
        return entry

    def entry(self, key: List[str], n: int = 3, r: int = 2, w: int = 2) -> Entry:
        """
        Создание Entry для указанного ключа в транзакции. Не выполняет никаких запросов.
        Можно использовать, если нет необходимости знать текущие значение и версию.
        
        :param key: ключ
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: Entry для указанного ключа
        """
        return Entry(self._session, key, 0, None, n, r, w, self._index)

    @property
    def index(self) -> int:
        """
        :return: индекс транзакции 
        """
        return self._index
