import asyncio
from decimal import Decimal
from typing import Optional, List, Tuple
from uuid import UUID, uuid1, uuid4

from acapella.kv import Session, Transaction


class Account:
    def __init__(self, id: UUID, balance: Decimal):
        self.id = id
        self.balance = balance

    def __repr__(self):
        return f'Account(id={self.id}, balance={self.balance})'


class Transfer:
    def __init__(self, timestamp: UUID, id_from: UUID, id_to: UUID, amount: Decimal):
        self.timestamp = timestamp
        self.id_from = id_from
        self.id_to = id_to
        self.amount = amount

    def __repr__(self):
        return f'Transfer(timestamp={self.timestamp}, id_from={self.id_from}, id_to={self.id_to}, amount={self.amount})'


class Accounts:
    PARTITION = ['examples', 'accounts']

    def __init__(self, session: Session):
        self._session = session

    async def drop_table(self):
        result = await self._session.range(self.PARTITION)
        batch = self._session.batch_manual()
        for entry in result:
            entry.set(None, reindex=True, batch=batch)
        await batch.send()

    async def create_table(self):
        pass

    async def save(self, tx: Transaction, account: Account):
        key, value = self._serialize(account)
        entry = tx.entry(self.PARTITION, key)
        await entry.set(value, reindex=True)

    async def get(self, tx: Transaction, id: UUID) -> Optional[Account]:
        key = self._serialize_key(id)
        entry = await tx.get_entry(self.PARTITION, key, watch=True)
        return self._deserialize(key, entry.value) if entry.value else None

    @classmethod
    def _serialize_key(cls, id: UUID) -> List[str]:
        return [str(id)]

    @classmethod
    def _serialize(cls, account: Account) -> Tuple[List[str], dict]:
        key = cls._serialize_key(account.id)
        value = {
            'balance': str(account.balance)
        }
        return key, value

    @staticmethod
    def _deserialize(key: List[str], value: dict) -> Account:
        return Account(
            id=UUID(key[0]),
            balance=Decimal(value['balance'])
        )


class Transfers:
    PARTITION = ['examples', 'transfers']

    def __init__(self, session: Session):
        self._session = session

    async def drop_table(self):
        result = await self._session.range(self.PARTITION)
        batch = self._session.batch_manual()
        for entry in result:
            entry.set(None, reindex=True, batch=batch)
        await batch.send()

    async def create_table(self):
        pass

    async def save(self, tx: Transaction, transfer: Transfer):
        key, value = self._serialize(transfer)
        entry = tx.entry(self.PARTITION, key)
        await entry.set(value, reindex=True)

    async def get(self, tx: Transaction, timestamp: UUID) -> Optional[Transfer]:
        key = self._serialize_key(timestamp)
        entry = await tx.get_entry(self.PARTITION, key, watch=True)
        return self._deserialize(key, entry.value) if entry.value else None

    async def get_all(self) -> List[Transfer]:
        result = await self._session.range(self.PARTITION)
        return [self._deserialize(e.clustering, e.value) for e in result if e.value is not None]

    @classmethod
    def _serialize_key(cls, timestamp: UUID) -> List[str]:
        return [str(timestamp)]

    @classmethod
    def _serialize(cls, transfer: Transfer) -> Tuple[List[str], dict]:
        key = cls._serialize_key(transfer.timestamp)
        value = {
            'id_from': str(transfer.id_from),
            'id_to': str(transfer.id_to),
            'amount': str(transfer.amount)
        }
        return key, value

    @staticmethod
    def _deserialize(key: List[str], value: dict) -> Transfer:
        return Transfer(
            timestamp=UUID(key[0]),
            id_from=UUID(value['id_from']),
            id_to=UUID(value['id_to']),
            amount=Decimal(value['amount'])
        )


class Model:
    def __init__(self, session: Session):
        self.session = session
        self.accounts = Accounts(session)
        self.transfers = Transfers(session)

    async def init(self):
        await self.accounts.drop_table()
        await self.transfers.drop_table()

        await self.accounts.create_table()
        await self.transfers.create_table()


class Transactor:
    def __init__(self, model: Model):
        self._model = model

    async def transaction(self, id_from: UUID, id_to: UUID, amount: Decimal):
        async with self._model.session.transaction() as tx:
            timestamp = uuid1()

            from_account = await self._model.accounts.get(tx, id_from)
            assert from_account.balance >= amount, "Source account must have required amount to transfer"
            from_account.balance -= amount
            await self._model.accounts.save(tx, from_account)

            to_account = await self._model.accounts.get(tx, id_to)
            to_account.balance += amount
            await self._model.accounts.save(tx, to_account)

            transfer = Transfer(timestamp, id_from, id_to, amount)
            await self._model.transfers.save(tx, transfer)


async def run():
    # init session
    session = Session(port=5678)
    await session.login('examples', 'examples')

    # init model
    model = Model(session)
    transactor = Transactor(model)
    await model.init()

    # inserting accounts
    account1 = Account(uuid4(), Decimal(100.0))
    account2 = Account(uuid4(), Decimal(10.0))
    async with session.transaction() as tx:
        await model.accounts.save(tx, account1)
        await model.accounts.save(tx, account2)

    # trying to perform bad transfer
    try:
        await transactor.transaction(account2.id, account1.id, Decimal(50.0))
    except AssertionError as e:
        print(e)

    # performing good transfers
    await transactor.transaction(account1.id, account2.id, Decimal(60.0))
    await transactor.transaction(account2.id, account1.id, Decimal(40.0))

    # now
    # account1 have balance = 80.0
    # account2 have balance = 30.0
    # transfers has two events:
    # 1 -> 60.0 -> 2
    # 2 -> 40.0 -> 1

    async with session.transaction() as tx:
        print('account 1', await model.accounts.get(tx, account1.id))
        print('account 2', await model.accounts.get(tx, account2.id))
        print('transfers', await model.transfers.get_all())


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(run())
