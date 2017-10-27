from typing import List, Optional, Dict

from acapella.kv import Entry
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
        self._session = session
        self._partition = partition

    async def query(self, query: Dict[str, QueryCondition], limit: Optional[int] = None) -> List[Entry]:
        url = f'/astorage/v2/kv/partition/{key_to_str(self._partition)}/index-query'
        response = await self._session.get(url, json={
            'params': {
               'limit': limit
            },
            'query': {field: cond.to_json() for field, cond in query.items()}
        })
        raise_if_error(response.status_code)
        data = response.json()
        return [Entry(self._session, self._partition, e['key'], 0, e.get('value'), 3, 2, 2, None) for e in data]
