from typed_python import OneOf, Alternative, ConstDict, TupleOf, Tuple
from object_database.schema import SchemaDefinition, ObjectId, ObjectFieldId, FieldId, IndexId, FieldDefinition

_heartbeatInterval = [5.0]


def setHeartbeatInterval(newInterval):
    _heartbeatInterval[0] = newInterval

def getHeartbeatInterval():
    return _heartbeatInterval[0]

ClientToServer = Alternative(
    "ClientToServer",
    TransactionData = {
        "writes": ConstDict(ObjectFieldId, OneOf(None, bytes)),
        "set_adds": ConstDict(IndexId, TupleOf(ObjectId)),
        "set_removes": ConstDict(IndexId, TupleOf(ObjectId)),
        "key_versions": TupleOf(ObjectFieldId),
        "index_versions": TupleOf(IndexId),
        "transaction_guid": int
        },
    CompleteTransaction = {
        "as_of_version": int,
        "transaction_guid": int
        },
    Heartbeat = {},
    DefineSchema = { 'name': str, 'definition': SchemaDefinition },
    LoadLazyObject = { 'schema': str, 'typename': str, 'identity': ObjectId },
    Subscribe = {
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str,bytes)),
        'isLazy': bool #load values when we first request them, instead of blocking on all the data.
        },
    Flush = {'guid': int},
    Authenticate = {'token': str}
    )


ServerToClient = Alternative(
    "ServerToClient",
    Initialize = {'transaction_num': int, 'connIdentity': ObjectId, 'identity_root': int},
    TransactionResult = {'transaction_guid': int, 'success': bool, 'badKey': OneOf(None, ObjectFieldId, IndexId, str) },
    SchemaMapping = { 'schema': str, 'mapping': ConstDict(FieldDefinition, int) },
    FlushResponse = {'guid': int},
    SubscriptionData = {
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str,bytes)),
        'values': ConstDict(ObjectFieldId, OneOf(None, bytes)), #value
        'index_values': ConstDict(ObjectFieldId, OneOf(None, bytes)),
        'identities': OneOf(None, TupleOf(ObjectId)), #the identities in play if this is an index-level subscription
        },
    LazyTransactionPriors = { 'writes': ConstDict(ObjectFieldId, OneOf(None, bytes)) },
    LazyLoadResponse = { 'identity': ObjectId, 'values': ConstDict(ObjectFieldId, OneOf(None, bytes)) },
    LazySubscriptionData = {
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, bytes)),
        'identities': TupleOf(ObjectId),
        'index_values': ConstDict(ObjectFieldId, OneOf(None, bytes))
        },
    SubscriptionComplete = {
        'schema': str,
        'typename': OneOf(None, str),
        'fieldname_and_value': OneOf(None, Tuple(str, bytes)),
        'tid': int #marker transaction id
        },
    SubscriptionIncrease = {
        'schema': str,
        'typename': str,
        'fieldname_and_value': Tuple(str, bytes),
        'identities': TupleOf(ObjectId)
        },
    Disconnected = {},
    Transaction = {
        "writes": ConstDict(ObjectFieldId, OneOf(None, bytes)),
        "set_adds": ConstDict(IndexId, TupleOf(ObjectId)),
        "set_removes": ConstDict(IndexId, TupleOf(ObjectId)),
        "transaction_id": int
    }
)
