from asyncio import Future
from typing import List, Optional, Dict, Tuple

import asyncio

from acapella.kv.BatchBase import BatchBase
from acapella.kv.utils.http import AsyncSession, key_to_str, raise_if_error


class BatchEntry(object):
    def __init__(self, new_value: Optional[object], old_version: Optional[int]):
        self.new_value = new_value
        self.old_version = old_version
        self.new_version = 0  # for response


class PartitionBatch(object):
    def __init__(self, n: int, r: int, w: int):
        self.n = n
        self.r = r
        self.w = w
        self.batch: Dict[Tuple[str, ...], BatchEntry] = {}

    def set(self, clustering: List[str], new_value: Optional[object]) -> BatchEntry:
        key = tuple(clustering)
        entry = BatchEntry(new_value, None)
        self.__assert_not_set(key)
        self.batch[key] = entry
        return entry

    def cas(self, clustering: List[str], new_value: Optional[object], old_version: int) -> BatchEntry:
        key = tuple(clustering)
        entry = BatchEntry(new_value, old_version)
        self.__assert_not_set(key)
        self.batch[key] = entry
        return entry

    def build_request_body(self) -> object:
        return [
            {
                'key': k,
                'value': e.new_value,
                'version': e.old_version
            }
            for k, e in self.batch.items()
        ]

    def apply_response(self, body: List[dict]):
        assert len(body) == len(self.batch)

        for item in body:
            key = tuple(item['key'])
            new_version = item['version']
            self.batch[key].new_version = new_version

    def __assert_not_set(self, clustering: Tuple[str, ...]):
        assert clustering not in self.batch, "Key can be added to batch only one time"


class BatchManual(BatchBase):
    def __init__(self, session: AsyncSession):
        self._session = session
        self._future = Future()
        self._in_process = True
        self._batch: Dict[Tuple[str, ...], PartitionBatch] = {}

    async def set(self, partition: List[str], clustering: List[str], new_value: Optional[object],
                  n: int, r: int, w: int) -> int:
        self._assert_in_process()
        partition_batch = self._batch.setdefault(tuple(partition), PartitionBatch(n, r, w))
        entry = partition_batch.set(clustering, new_value)
        await self._future
        return entry.new_version

    async def cas(self, partition: List[str], clustering: List[str], new_value: Optional[object], old_version: int,
                  n: int, r: int, w: int) -> int:
        self._assert_in_process()
        partition_batch = self._batch.setdefault(tuple(partition), PartitionBatch(n, r, w))
        entry = partition_batch.cas(clustering, new_value, old_version)
        await self._future
        return entry.new_version

    async def send(self):
        # если в батче ничего нет, то скорее всего все запросы были
        # закинуты в event-loop и ещё не выполнились, так что нужно
        # дать им возможность это сделать
        if len(self._batch) == 0:
            # более-менее стандартный способ сделать yield
            await asyncio.sleep(0)

        self._in_process = False
        requests = []

        for k, b in self._batch.items():
            requests.append(self._send_partition(k, b))
        for request in requests:
            await request

        self._future.set_result(None)

    async def _send_partition(self, partition: Tuple[str, ...], batch: PartitionBatch):
        url = f'/v2/kv/partition/{key_to_str(partition)}'
        response = await self._session.put(url, params={
            'n': batch.n,
            'r': batch.r,
            'w': batch.w,
        }, json=batch.build_request_body())
        raise_if_error(response.status_code)
        batch.apply_response(response.json())

    def _assert_in_process(self):
        assert self._in_process, "Batch can be used only one time"
