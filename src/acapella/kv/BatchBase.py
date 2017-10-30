from typing import Optional, List


class BatchBase(object):
    """
    Базовый класс для формирования батч-запрсов.
    """

    async def set(self, partition: List[str], clustering: List[str], new_value: Optional[object],
                  reindex: bool, n: int, r: int, w: int) -> int:
        """
        Добавляет set-запрос в батч. Метод возвращает управление, когда батч будет выполнен.
        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param new_value: новое значение
        :param reindex: переиндексировать ключ с новым значением?
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: новая версия
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        raise NotImplementedError()

    async def cas(self, partition: List[str], clustering: List[str], new_value: Optional[object],
                  old_version: int, reindex: bool, n: int, r: int, w: int) -> int:
        """
        Добавляет cas-запрос в батч. Метод возвращает управление, когда батч будет выполнен.
        :param partition: распределительный ключ
        :param clustering: сортируемый ключ
        :param new_value: новое значение
        :param old_version: версия для сравнения
        :param reindex: переиндексировать ключ с новым значением?
        :param n: количество реплик
        :param r: количество ответов для подтверждения чтения
        :param w: количество ответов для подтверждения записи
        :return: новая версия
        :raise CasError: когда текущая версия значения не совпала с указанной
        :raise TimeoutError: когда время ожидания запроса истекло
        :raise KvError: когда произошла неизвестная ошибка на сервере
        """
        raise NotImplementedError()
