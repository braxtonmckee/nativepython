from typed_python import *
from object_database.schema import SchemaDefinition


_heartbeatInterval = [5.0]


def setHeartbeatInterval(newInterval):
    _heartbeatInterval[0] = newInterval


def getHeartbeatInterval():
    return _heartbeatInterval[0]


ClientToServer = Alternative(
    "ClientToServer",
    TransactionData={
        "writes": ConstDict(str, OneOf(None, str)),
        "set_adds": ConstDict(str, TupleOf(str)),
        "set_removes": ConstDict(str, TupleOf(str)),
        "key_versions": TupleOf(str),
        "index_versions": TupleOf(str),
        "transaction_guid": str,
        "channel_guid": str
    },
    CompleteTransaction={
        "as_of_version": int,
        "transaction_guid": str,
        "channel_guid": str
    },
    Heartbeat={},
    DefineSchema={ 'name': str, 'definition': SchemaDefinition, "channel_guid": str },
    LoadLazyObject={ 'schema': str, 'typename': str, 'identity': str, 'channel_guid': str },
    Subscribe={
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, str)),
        'isLazy': bool,  # load values when we first request them, instead of blocking on all the data.
        'channel_guid': str
    },
    Flush={'channel_guid': str, 'guid': str},
    Authenticate={'token': str, "channel_guid": str},
    AddChannel={'channel_guid': str},
    DropChannel={'channel_guid': str}
)


ServerToClient = Alternative(
    "ServerToClient",
    Initialize={'transaction_num': int, 'connIdentity': str, 'identity_root': int, 'channel_guid': str},
    TransactionResult={'transaction_guid': str, 'success': bool, 'badKey': OneOf(None, str), 'channel_guid': str },
    FlushResponse={'channel_guid': str, 'guid': str},
    SubscriptionData={
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, str)),
        'values': ConstDict(str, OneOf(None, str)),  # value
        'index_values': ConstDict(str, OneOf(None, str)),
        'identities': OneOf(None, TupleOf(str)),  # the identities in play if this is an index-level subscription
        'channel_guid': str
    },
    LazyTransactionPriors={ 'writes': ConstDict(str, OneOf(None, str)), 'channel_guid': str },
    LazyLoadResponse={ 'identity': str, 'values': ConstDict(str, OneOf(None, str)), 'channel_guid': str },
    LazySubscriptionData={
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, str)),
        'identities': TupleOf(str),
        'index_values': ConstDict(str, OneOf(None, str)),
        'channel_guid': str
    },
    SubscriptionComplete={
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, str)),
        'tid': int,  # marker transaction id
        'channel_guid': str
    },
    SubscriptionIncrease={
        'schema': str,
        'typename': str,
        'fieldname_and_value': Tuple(str, str),
        'identities': TupleOf(str)
    },
    Disconnected={},
    Transaction={
        "writes": ConstDict(str, OneOf(None, str)),
        "set_adds": ConstDict(str, TupleOf(str)),
        "set_removes": ConstDict(str, TupleOf(str)),
        "transaction_id": int
    }
)
