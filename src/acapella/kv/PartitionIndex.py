from typing import List, Optional, Dict

from acapella.kv import Entry
from acapella.kv.IndexField import IndexField
from acapella.kv.consts import API_PREFIX
from acapella.kv.utils.http import AsyncSession, raise_if_error, key_to_str


class QueryCondition(object):
    def __init__(self, eq: Optional[any] = None, from_: Optional[any] = None, to_: Optional[any] = None):
        self.eq = eq
        self.from_ = from_
        self.to_ = to_

    def __repr__(self) -> str:
        return f'QueryCondition(eq={self.eq}, from={self.from_}, to={self.to_}'

    def to_json(self):
        return {
            'eq': self.eq,
            'from': self.from_,
            'to': self.to_
        }


class PartitionIndex(object):
    def __init__(self, session: AsyncSession, partition: List[str]):
        assert len(partition) >= 2, "Indexed partition must be in format: [<user>, <keyspace>, ...]"

        self._session = session
        self._user = partition[0]
        self._keyspace = partition[1]
        self._partition = partition

    async def query(self, query: Dict[str, QueryCondition], limit: Optional[int] = None) -> List[Entry]:
        url = f'{API_PREFIX}/v2/kv/partition/{key_to_str(self._partition)}/index-query'
        response = await self._session.get(url, json={
            'params': {
               'limit': limit
            },
            'query': {field: cond.to_json() for field, cond in query.items()}
        })
        raise_if_error(response.status)
        data = await response.json()
        return [Entry(self._session, self._partition, e['key'], 0, e.get('value'), 3, 2, 2, None) for e in data]

    async def set_index(self, tag: int, fields: List[IndexField]):
        url = f'{API_PREFIX}/v2/users/{self._user}/keyspaces/{self._keyspace}/indexes/{tag}'
        response = await self._session.put(url, json={
            'fields': [f.to_json() for f in fields]
        })
        raise_if_error(response.status)

    async def get_indexes(self) -> Dict[int, List[IndexField]]:
        url = f'{API_PREFIX}/v2/users/{self._user}/keyspaces/{self._keyspace}/indexes'
        response = await self._session.get(url)
        raise_if_error(response.status)
        data = await response.json()
        indexes = data['indexes']
        return {int(tag): [IndexField.from_json(field) for field in index['fields']] for tag, index in indexes.items()}
