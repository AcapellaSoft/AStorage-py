from enum import Enum


class IndexFieldType(Enum):
    string = 'string'
    number = 'number'


class IndexFieldOrder(Enum):
    ascending = 'ascending'
    descending = 'descending'


class IndexField(object):
    def __init__(self, name: str, type_: IndexFieldType, order: IndexFieldOrder):
        self.name = name
        self.type = type_
        self.order = order

    def to_json(self):
        return {
            'name': self.name,
            'type': self.type.value,
            'order': self.order.value,
        }

    @classmethod
    def from_json(cls, data) -> 'IndexField':
        return IndexField(
            name=data['name'],
            type_=IndexFieldType[data['type']],
            order=IndexFieldOrder[data['order']],
        )

    def __repr__(self) -> str:
        return f'IndexField(name={self.name}, type={self.type}, order={self.order})'

    def __eq__(self, other) -> bool:
        if not isinstance(other, IndexField):
            return False
        return self.name == other.name and self.type == other.type and self.order == other.order
