Описание
--------

Асинхронный Python-клиент для Key-Value базы данных [AcapellaDB](http://acapella.ru/acapelladb_doc/data-model/).

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

Для хранения сложных структур данных введены две части ключа: `partition` и `clustering`. Первый используется для распределения данных по кластеру. Все clustering-ключи в пределах одного partition-ключа лежат вместе на каждой реплике, что обеспечивает возможность выборок и batch-запросов. 

Пример работы со списком пользователей внутри одного partition'а:

```python
>>> # создание списка
>>> await session.entry(partition = ["users"], clustering = ["first"]).set({
>>>     'age': 25
>>> })
>>> await session.entry(partition = ["users"], clustering = ["second"]).set({
>>>     'age': 32
>>> })
>>> await session.entry(partition = ["users"], clustering = ["third"]).set({
>>>     'age': 21
>>> })

>>> # выборка всех пользователей
>>> data = await session.range(partition = ["users"])
>>> for e in data:
>>>     print(f'{e.key[0]}: {e.value.age}')
first: 25
second: 32
third: 21

>>> # выборка первых 2-х пользователей
>>> data = await session.range(partition = ["users"], limit = 2)
>>> for e in data:
>>>     print(f'{e.key[0]}: {e.value.age}')
first: 25
second: 32
```

Пример работы с очередью:

```python
>>> # запись событий в очередь по 10 штук
>>> for i in range(10):
>>>     # записи производятся в батч, а потом выполняется один запрос
>>>     batch = BatchManual()
>>>     for i in range(10):
>>>         key = str(uuid1())
>>>         e = session.entry(partition = ["queue-1"], clustering = [key])
>>>         await e.set(value = i, batch = batch)
>>>     # выполнение батча
>>>     await batch.send()

>>> # чтение событий из очереди по 10 штук
>>> first = []
>>> for i in rannge(10):
>>>     data = await session.range(partition = ["queue-1"], first = first, limit = 10)
>>>     for e in data:
>>>         print(f'{e.key}: {e.value}')
>>>     first = data[len(data) - 1].key
>>> 
['be2a5d92-8cc0-11e7-8bb2-40e230b5623b']: 0
['be2a6058-8cc0-11e7-8bb2-40e230b5623b']: 1
['be2a61f2-8cc0-11e7-8bb2-40e230b5623b']: 2
...
['be2ae000-8cc0-11e7-8bb2-40e230b5623b']: 99

>>> # выборка всех событий за определённый интервал времени
>>> data = await session.range(
>>>     partition = ["queue-1"],
>>>     first = ['be2a61f2-8cc0-11e7-8bb2-40e230b5623b'],
>>>     last =  ['be2a7fac-8cc0-11e7-8bb2-40e230b5623b']
>>> )
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

```python
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

