import asyncio
from datetime import datetime
from typing import Optional, List, Tuple
from uuid import UUID, uuid4

from acapelladb import Session
from acapelladb.IndexField import IndexField, IndexFieldType, IndexFieldOrder
from acapelladb.PartitionIndex import QueryCondition
from examples.credentials import USER_NAME, PASSWORD


class Person:
    def __init__(self, id: UUID, first_name: str, last_name: str):
        self.id = id
        self.first_name = first_name
        self.last_name = last_name

    def __repr__(self):
        return f'Person(id={self.id}, first_name={self.first_name}, last_name={self.last_name})'


class Apartment:
    def __init__(self, id: UUID, address: str):
        self.id = id
        self.address = address

    def __repr__(self):
        return f'Apartment(id={self.id}, address={self.address})'


class Registration:
    def __init__(self, person_id: UUID, apartment_id: UUID, created_at: datetime):
        self.person_id = person_id
        self.apartment_id = apartment_id
        self.created_at = created_at

    def __repr__(self):
        return f'Registration(' \
                   f'person_id={self.person_id}, ' \
                   f'apartment_id={self.apartment_id}, ' \
                   f'created_at={self.created_at}' \
               f')'


class Persons:
    # first key part is user-id
    # second key part is keyspace
    PARTITION = [USER_NAME, 'persons']

    def __init__(self, session: Session):
        self._session = session

    async def drop_table(self):
        result = await self._session.range(self.PARTITION)
        batch = self._session.batch_manual()
        for entry in result:
            entry.set(None, reindex=True, batch=batch)
        await batch.send()

    async def create_table(self):
        # indexes are assigned to keyspace, which is computed out of partition like this:
        # [user-id, keyspace, some, custom, parts, ...]
        # all keys in this keyspace will be indexed using this indexes
        indexes = self._session.partition_index(self.PARTITION)
        await indexes.set_index(1, [IndexField('first_name', IndexFieldType.string, IndexFieldOrder.ascending)])
        await indexes.set_index(2, [IndexField('last_name', IndexFieldType.string, IndexFieldOrder.ascending)])

    async def save(self, person: Person):
        key, value = self._serialize(person)
        entry = self._session.entry(self.PARTITION, key)
        await entry.set(value, reindex=True)

    async def get(self, id: UUID) -> Optional[Person]:
        key = self._serialize_key(id)
        entry = await self._session.get_entry(self.PARTITION, key)
        return self._deserialize(key, entry.value) if entry.value else None

    async def get_by_first_name(self, first_name: str) -> List[Person]:
        result = await self._session.partition_index(self.PARTITION).query({
            'first_name': QueryCondition(eq=first_name)
        })
        return [self._deserialize(entry.clustering, entry.value) for entry in result]

    async def get_by_last_name(self, last_name: str) -> List[Person]:
        result = await self._session.partition_index(self.PARTITION).query({
            'last_name': QueryCondition(eq=last_name)
        })
        return [self._deserialize(entry.clustering, entry.value) for entry in result]

    @classmethod
    def _serialize_key(cls, id: UUID) -> List[str]:
        return [str(id)]

    @classmethod
    def _serialize(cls, person: Person) -> Tuple[List[str], dict]:
        key = cls._serialize_key(person.id)
        value = {
            'first_name': person.first_name,
            'last_name': person.last_name
        }
        return key, value

    @staticmethod
    def _deserialize(key: List[str], value: dict) -> Person:
        return Person(
            id=UUID(key[0]),
            first_name=value['first_name'],
            last_name=value['last_name']
        )


class Apartments:
    PARTITION = [USER_NAME, 'apartments']

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

    async def save(self, apartment: Apartment):
        key, value = self._serialize(apartment)
        entry = self._session.entry(self.PARTITION, key)
        await entry.set(value, reindex=True)

    async def get(self, id: UUID) -> Optional[Apartment]:
        key = self._serialize_key(id)
        entry = await self._session.get_entry(self.PARTITION, key)
        return self._deserialize(key, entry.value) if entry.value else None

    @classmethod
    def _serialize_key(cls, id: UUID) -> List[str]:
        return [str(id)]

    @classmethod
    def _serialize(cls, apartment: Apartment) -> Tuple[List[str], dict]:
        key = cls._serialize_key(apartment.id)
        value = {
            'address': apartment.address
        }
        return key, value

    @staticmethod
    def _deserialize(key: List[str], value: dict) -> Apartment:
        return Apartment(
            id=UUID(key[0]),
            address=value['address']
        )


class Registrations:
    PARTITION = [USER_NAME, 'registrations']

    def __init__(self, session: Session):
        self._session = session

    async def drop_table(self):
        result = await self._session.range(self.PARTITION)
        batch = self._session.batch_manual()
        for entry in result:
            entry.set(None, reindex=True, batch=batch)
        await batch.send()

    async def create_table(self):
        indexes = self._session.partition_index(self.PARTITION)
        await indexes.set_index(1, [IndexField('person_id', IndexFieldType.string, IndexFieldOrder.ascending)])
        await indexes.set_index(2, [IndexField('apartment_id', IndexFieldType.string, IndexFieldOrder.ascending)])
        await indexes.set_index(3, [IndexField('created_at', IndexFieldType.number, IndexFieldOrder.ascending)])

    async def save(self, registration: Registration):
        key, value = self._serialize(registration)
        entry = self._session.entry(self.PARTITION, key)
        await entry.set(value, reindex=True)

    async def get(self, person_id: UUID, apartment_id: UUID) -> Optional[Registration]:
        key = self._serialize_key(person_id, apartment_id)
        entry = await self._session.get_entry(self.PARTITION, key)
        return self._deserialize(key, entry.value) if entry.value else None

    async def get_by_person(self, person_id: UUID) -> List[Registration]:
        result = await self._session.partition_index(self.PARTITION).query({
            'person_id': QueryCondition(eq=str(person_id))
        })
        return [self._deserialize(entry.clustering, entry.value) for entry in result]

    async def get_by_apartment(self, apartment_id: UUID) -> List[Registration]:
        result = await self._session.partition_index(self.PARTITION).query({
            'apartment_id': QueryCondition(eq=str(apartment_id))
        })
        return [self._deserialize(entry.clustering, entry.value) for entry in result]

    async def get_since(self, since: datetime) -> List[Registration]:
        result = await self._session.partition_index(self.PARTITION).query({
            'created_at': QueryCondition(from_=since.timestamp())
        })
        return [self._deserialize(entry.clustering, entry.value) for entry in result]

    @classmethod
    def _serialize_key(cls, person_id: UUID, apartment_id: UUID) -> List[str]:
        return [str(person_id), str(apartment_id)]

    @classmethod
    def _serialize(cls, registration: Registration) -> Tuple[List[str], dict]:
        key = cls._serialize_key(registration.person_id, registration.apartment_id)
        value = {
            'person_id': str(registration.person_id),
            'apartment_id': str(registration.apartment_id),
            'created_at': registration.created_at.timestamp()
        }
        return key, value

    @staticmethod
    def _deserialize(key: List[str], value: dict) -> Registration:
        return Registration(
            person_id=UUID(key[0]),
            apartment_id=UUID(key[1]),
            created_at=datetime.fromtimestamp(value['created_at'])
        )


async def run():
    # init database
    session = Session(host='api.acapella.ru', port=5678)
    await session.login(USER_NAME, PASSWORD)

    # init persons table
    persons = Persons(session)
    await persons.drop_table()
    await persons.create_table()

    # init apartments table
    apartments = Apartments(session)
    await apartments.drop_table()
    await apartments.create_table()

    # init registrations
    registrations = Registrations(session)
    await registrations.drop_table()
    await registrations.create_table()

    # inserting persons
    person1 = Person(id=uuid4(), first_name='John', last_name='Smith')
    person2 = Person(id=uuid4(), first_name='Mary', last_name='Smith')
    person3 = Person(id=uuid4(), first_name='David', last_name='Brown')
    person4 = Person(id=uuid4(), first_name='David', last_name='Thompson')
    await persons.save(person1)
    await persons.save(person2)
    await persons.save(person3)
    await persons.save(person4)

    # inserting apartments
    apartment1 = Apartment(id=uuid4(), address='123 6th St. Melbourne, FL 32904')
    apartment2 = Apartment(id=uuid4(), address='4 Goldfield Rd. Honolulu, HI 96815')
    apartment3 = Apartment(id=uuid4(), address='70 Bowman St. South Windsor, CT 06074')
    apartment4 = Apartment(id=uuid4(), address='514 S. Magnolia St. Orlando, FL 32806')
    await apartments.save(apartment1)
    await apartments.save(apartment2)
    await apartments.save(apartment3)
    await apartments.save(apartment4)

    # inserting registrations
    registration1 = Registration(
        person_id=person1.id,
        apartment_id=apartment1.id,
        created_at=datetime(year=2015, month=4, day=21)
    )
    registration2 = Registration(
        person_id=person2.id,
        apartment_id=apartment1.id,
        created_at=datetime(year=2015, month=4, day=22)
    )
    registration3 = Registration(
        person_id=person3.id,
        apartment_id=apartment2.id,
        created_at=datetime(year=2016, month=8, day=14)
    )
    registration4 = Registration(
        person_id=person4.id,
        apartment_id=apartment3.id,
        created_at=datetime(year=2017, month=3, day=1)
    )
    registration5 = Registration(
        person_id=person4.id,
        apartment_id=apartment4.id,
        created_at=datetime(year=2017, month=11, day=25)
    )
    await registrations.save(registration1)
    await registrations.save(registration2)
    await registrations.save(registration3)
    await registrations.save(registration4)
    await registrations.save(registration5)

    # query by last name = Smith
    result = await persons.get_by_last_name('Smith')
    # prints John Smith, Mary Smith
    print(result)

    # query by first name = David
    result = await persons.get_by_first_name('David')
    # prints David Thompson, David Brown
    print(result)

    # query registrations for apartment1
    result = await registrations.get_by_apartment(apartment1.id)
    result = [await persons.get(entry.person_id) for entry in result]
    # prints John Smith, Mary Smith
    print(result)

    # query registrations for person4
    result = await registrations.get_by_person(person4.id)
    result = [await apartments.get(entry.apartment_id) for entry in result]
    # prints
    # 70 Bowman St. South Windsor, CT 06074
    # 514 S. Magnolia St. Orlando, FL 32806
    print(result)

    # query registrations since year 2016 (inclusive)
    result = await registrations.get_since(datetime(year=2016, month=1, day=1))
    # prints registrations 3, 4 and 5
    print(result)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(run())
