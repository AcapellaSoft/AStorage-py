Описание
--------

Асинхронный Python-клиент для Key-Value базы данных [AStorage](http://acapella.ru/nosql/storage.html).

Примеры использования
---------------------

Для начала работы необходимо создать сессию:

```python
>>> session = Session(host = 'localhost', port = 12000)
```

Базовые GET/SET операции с ключами производятся с помощью класса Entry:

```python
>>> # создание объекта Entry, ключи являются массивом строк
>>> entry = session.entry(["foo", "bar"])

>>> # установка значения
>>> await entry.set("value 1")

>>> # установка значения с условием совпадения версии
>>> await entry.cas("value 2")

>>> # получение значения по ключу и сохранение в Entry
>>> entry = await session.get_entry(["foo", "bar"])
>>> print(f'value = "{entry.value}", version = {entry.version}')
value = "value 2", version = 2
```

Для работы с деревьями (DT, Distributed Tree) используются классы Tree и Cursor:

```python
>>> # создание дерева
>>> tree = session.tree(["test", "tree"])

>>> # заполнение дерева
>>> await tree.cursor(["a"]).set("1")
>>> await tree.cursor(["b"]).set("2")
>>> await tree.cursor(["c"]).set("3")
>>> await tree.cursor(["d"]).set("4")
>>> await tree.cursor(["e"]).set("5")

>>> # получение следующего ключа в дереве
>>> next = await tree.cursor(["b"]).next()  # next.key = ["c"]

>>> # получение предыдущего ключа в дереве
>>> prev = await tree.cursor(["b"]).next()  # next.key = ["a"]

>>> # выборка данных по заданным ограничениям
>>> data = await tree.range(first = ["a"], last = ["d"], limit = 2)
>>> print([e.key for e in data])
[['b'], ['c']]
```

Так же для всех операций доступен транзакционный режим. Транзакции можно использовать в двух режимах: 

1) как context manager

```python
>>> async with session.transaction() as tx:
>>>     # использование транзакции
>>>     entry = await tx.get_entry(["foo", "bar"])
>>>     await entry.cas("value 3")
```

2) в "ручном" режиме, необходимо явно вызвать commit/rollback при завершении работы с транзакцией

```
>>> # создание транзакции
>>> tx = await session.transaction_manual()
>>> try:
>>>     # использование транзакции
>>>     entry = await tx.get_entry(["foo", "bar"])
>>>     await entry.cas("value 3")
>>>     # commit, если не произошло исключений
>>>     await tx.commit()
>>> except Exception:
>>>     # rollback, если произошла какая-либо ошибка
>>>     await tx.rollback()
```

Больше примеров использования можно найти в [тестах](tests).

