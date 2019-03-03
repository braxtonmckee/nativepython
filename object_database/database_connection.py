#   Copyright 2018 Braxton Mckee
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

from object_database.schema import Schema, ObjectFieldId, IndexId, FieldDefinition
from object_database.messages import ClientToServer, ServerToClient, SchemaDefinition, getHeartbeatInterval
from object_database.core_schema import core_schema
from object_database.view import View, Transaction, _cur_view, SerializedDatabaseValue
from object_database.identity import IdentityProducer
import object_database.keymapping as keymapping
from typed_python.SerializationContext import SerializationContext
from typed_python.Codebase import Codebase as TypedPythonCodebase
from typed_python import Alternative, Dict, OneOf

import queue
import threading
import logging
import traceback
import time

from object_database.view import DisconnectedException


class Everything:
    """Singleton to mark subscription to everything in a slice."""


TransactionResult = Alternative(
    "TransactionResult",
    Success = {},
    RevisionConflict = {'key': OneOf(str, ObjectFieldId, IndexId)},
    Disconnected = {}
    )


class VersionedBase:
    def _best_version_offset_for(self, version):
        i = len(self.version_numbers) - 1

        while i >= 0:
            if self.version_numbers[i] <= version:
                return i
            i -= 1

        return None

    def isEmpty(self):
        return not self.version_numbers

    def validVersionIncoming(self, version_read, transaction_id):
        if not self.version_numbers:
            return True
        top = self.version_numbers[-1]
        assert transaction_id > version_read
        return version_read >= top

    def hasVersionInfoNewerThan(self, tid):
        if not self.version_numbers:
            return False
        return tid < self.version_numbers[-1]

    def newestValue(self):
        if self.version_numbers:
            return self.valueForVersion(self.version_numbers[-1])
        else:
            return self.valueForVersion(None)


class VersionedValue(VersionedBase):
    def __init__(self):
        self.version_numbers = []
        self.values = []

    def setVersionedValue(self, version_number, val):
        self.version_numbers.append(version_number)
        self.values.append(val)

    def valueForVersion(self, version):
        i = self._best_version_offset_for(version)

        if i is None:
            return None

        return self.values[i]

    def needsToTrack(self):
        return len(self.version_numbers) != 1 or self.values[0].serializedByteRep is None

    def cleanup(self, version_number):
        if not self.values:
            return True

        while self.values and self.version_numbers[0] < version_number:
            if len(self.values) == 1:
                if self.values[0].serializedByteRep is None:
                    # this value was deleted and we can just remove this whole entry
                    return True
                else:
                    self.version_numbers[0] = version_number
            else:
                if self.version_numbers[1] <= version_number:
                    self.values.pop(0)
                    self.version_numbers.pop(0)
                else:
                    self.version_numbers[0] = version_number

    def __repr__(self):
        return "VersionedValue(ids=%s)" % (self.version_numbers,)


class VersionedSet(VersionedBase):
    # values in sets are always strings
    def __init__(self):
        self.version_numbers = []
        self.adds = []
        self.removes = []

    def setVersionedAddsAndRemoves(self, version, adds, removes):
        assert not adds or not removes
        assert adds or removes
        assert isinstance(adds, set)
        assert isinstance(removes, set)

        if not self.version_numbers:
            if removes:
                adds = set(adds)
                adds.difference_update(removes)
                removes = set()

        self.adds.append(adds)
        self.removes.append(removes)
        self.version_numbers.append(version)

    def needsToTrack(self):
        return len(self.version_numbers) != 1 or not (len(self.adds[0]) or len(self.removes[0]))

    def updateVersionedAdds(self, version, adds):
        if not self.version_numbers or self.version_numbers[-1] != version:
            assert not self.version_numbers or self.version_numbers[-1] < version
            self.setVersionedAddsAndRemoves(version, adds, set())
        else:
            # someone could be iterating over this set in another thread
            new_last_adds = set(self.adds[-1])
            new_last_adds.update(adds)
            self.adds[-1] = new_last_adds

    def cleanup(self, version_number):
        if not self.version_numbers:
            return True

        while self.version_numbers and self.version_numbers[0] < version_number:
            if len(self.version_numbers) == 1:
                if not self.adds[0] and not self.removes[0]:
                    # this value was deleted and we can just remove this whole entry
                    return True
                else:
                    self.version_numbers[0] = version_number
            else:
                if self.version_numbers[1] <= version_number:
                    # merge slot 0 into slot 1
                    # the new set should have no removes, and only adds
                    assert not self.removes[0], (self.adds[0], self.removes[0])

                    new_set = set(self.adds[0])
                    new_set.update(self.adds[1])
                    new_set.difference_update(self.removes[1])

                    self.adds.pop(0)
                    self.removes.pop(0)
                    self.version_numbers.pop(0)

                    self.adds[0] = new_set
                    self.removes[0] = set()
                else:
                    self.version_numbers[0] = version_number

    def valueForVersion(self, version):
        ix = self._best_version_offset_for(version)

        if ix is None:
            return SetWithEdits(set(), set(), set())

        return SetWithEdits(set(), self.adds[:ix+1], self.removes[:ix+1])

    def __repr__(self):
        return "VersionedSet(ids=%s, adds=%s, removes=%s)" % (self.version_numbers, self.adds, self.removes)


class SetWithEdits:
    AGRESSIVELY_CHECK_SET_ADDS = False

    def __init__(self, s, adds, removes):
        self.s = s
        self.adds = adds
        self.removes = removes

    def toSet(self):
        res = set(self.s)
        for i in range(len(self.adds)):
            res.update(self.adds[i])
            res.difference_update(self.removes[i])
        return res

    def pickAny(self, toAvoid):
        removed = set()

        for i in reversed(range(len(self.adds))):
            if SetWithEdits.AGRESSIVELY_CHECK_SET_ADDS:
                adds = set(self.adds[i])

            for a in self.adds[i]:
                if a not in removed and a not in toAvoid:
                    return a

            if SetWithEdits.AGRESSIVELY_CHECK_SET_ADDS:
                time.sleep(0.0001)
                assert self.adds[i] == adds

            removed.update(self.removes[i])

        for a in self.s:
            if a not in removed and a not in toAvoid:
                return a


class ManyVersionedObjects:
    def __init__(self):
        # for each version number we have outstanding
        self._version_number_refcount = {}

        self._min_reffed_version_number = None

        # for each version number, the set of keys that are set with it
        self._version_number_objects = {}

        # for each key, a VersionedValue or VersionedSet
        self._versioned_objects = {}

    def keycount(self):
        return len(self._versioned_objects)

    def versionIncref(self, version_number):
        if version_number not in self._version_number_refcount:
            self._version_number_refcount[version_number] = 1

            if self._min_reffed_version_number is None:
                self._min_reffed_version_number = version_number
            else:
                self._min_reffed_version_number = min(version_number, self._min_reffed_version_number)
        else:
            self._version_number_refcount[version_number] += 1

    def versionDecref(self, version_number):
        assert version_number in self._version_number_refcount

        self._version_number_refcount[version_number] -= 1

        assert self._version_number_refcount[version_number] >= 0

        if self._version_number_refcount[version_number] == 0:
            del self._version_number_refcount[version_number]

            if version_number == self._min_reffed_version_number:
                if not self._version_number_refcount:
                    self._min_reffed_version_number = None
                else:
                    self._min_reffed_version_number = min(self._version_number_refcount)

    def setForVersion(self, key, version_number):
        if key in self._versioned_objects:
            return self._versioned_objects[key].valueForVersion(version_number)

        return SetWithEdits(set(), set(), set())

    def hasDataForKey(self, key):
        return key in self._versioned_objects

    def valueForVersion(self, key, version_number):
        return self._versioned_objects[key].valueForVersion(version_number)

    def _object_has_version(self, key, version_number):
        if version_number not in self._version_number_objects:
            self._version_number_objects[version_number] = set()

        self._version_number_objects[version_number].add(key)

    def setVersionedValue(self, key, version_number, serialized_val):
        self._object_has_version(key, version_number)

        if key not in self._versioned_objects:
            self._versioned_objects[key] = VersionedValue()

        versioned = self._versioned_objects[key]

        initialValue = versioned.newestValue()

        versioned.setVersionedValue(version_number, SerializedDatabaseValue(serialized_val, {}))

        return initialValue

    def setVersionedAddsAndRemoves(self, key, version_number, adds, removes):
        self._object_has_version(key, version_number)

        if key not in self._versioned_objects:
            self._versioned_objects[key] = VersionedSet()

        if adds or removes:
            self._versioned_objects[key].setVersionedAddsAndRemoves(version_number, adds, removes)

    def setVersionedTailValueStringified(self, key, serialized_val):
        if key not in self._versioned_objects:
            self._object_has_version(key, -1)
            self._versioned_objects[key] = VersionedValue()
            self._versioned_objects[key].setVersionedValue(-1, SerializedDatabaseValue(serialized_val, {}))

    def updateVersionedAdds(self, key, version_number, adds):
        self._object_has_version(key, version_number)

        if key not in self._versioned_objects:
            self._versioned_objects[key] = VersionedSet()
            self._versioned_objects[key].setVersionedAddsAndRemoves(version_number, adds, set())
        else:
            self._versioned_objects[key].updateVersionedAdds(version_number, adds)

    def cleanup(self, curTransactionId):
        """Get rid of old objects we don't need to keep around and increase the min_transaction_id"""
        if self._min_reffed_version_number is not None:
            lowestId = min(self._min_reffed_version_number, curTransactionId)
        else:
            lowestId = curTransactionId

        if self._version_number_objects:
            while min(self._version_number_objects) < lowestId:
                toCollapse = min(self._version_number_objects)

                for key in self._version_number_objects[toCollapse]:
                    if key not in self._versioned_objects:
                        pass
                    elif self._versioned_objects[key].cleanup(lowestId):
                        del self._versioned_objects[key]
                    else:
                        if self._versioned_objects[key].needsToTrack():
                            self._object_has_version(key, lowestId)

                del self._version_number_objects[toCollapse]


class DatabaseConnection:
    def __init__(self, channel):
        self._channel = channel
        self._transaction_callbacks = {}

        self._lock = threading.Lock()

        # transaction of what's in the KV store
        self._cur_transaction_num = 0

        # a datastructure that keeps track of all the different versions of the objects
        # we have mapped in.
        self._versioned_data = ManyVersionedObjects()

        # a map from lazy object id to (schema, typename)
        self._lazy_objects = {}
        self._lazy_object_read_blocks = {}

        self.initialized = threading.Event()
        self.disconnected = threading.Event()

        # for each schema name we've sent, an event that's triggered
        # when the server has acknowledged the schema and given us a definition
        self._schema_response_events = {}
        self._fields_to_field_ids = Dict(FieldDefinition, int)()
        self._field_id_to_field_def = Dict(int, FieldDefinition)()

        self.connectionObject = None

        # transaction handlers. These must be nonblocking since we call them under lock
        self._onTransactionHandlers = []

        self._flushEvents = {}

        # Map: schema.name -> schema
        self._schemas = {}

        self._messages_received = 0

        self._pendingSubscriptions = {}

        # if we have object-level subscriptions to a particular type (e.g. not everything)
        # then, this is from (schema, typename) -> {object_id -> transaction_id} so that
        # we can tell when the subscription should become valid. Subscriptions are permanent
        # otherwise, if we're subscribed, it's 'Everything'
        self._schema_and_typename_to_subscription_set = {}

        # from (schema,typename,field_val) -> {'values', 'index_values', 'identities'}
        self._subscription_buildup = {}

        self._channel.setServerToClientHandler(self._onMessage)

        self._flushIx = 0

        self._largeSubscriptionHeartbeatDelay = 0

        self.serializationContext = TypedPythonCodebase.coreSerializationContext().withoutCompression()

        self._logger = logging.getLogger(__name__)



    def registerOnTransactionHandler(self, handler):
        self._onTransactionHandlers.append(handler)

    def setSerializationContext(self, context):
        assert isinstance(context, SerializationContext), context
        self.serializationContext = context.withoutCompression()
        return self

    def serializeFromModule(self, module):
        """Give the project root we want to serialize from."""
        self.setSerializationContext(
            TypedPythonCodebase.FromRootlevelModule(module).serializationContext
        )

    def _stopHeartbeating(self):
        self._channel._stopHeartbeating()

    def disconnect(self):
        self.disconnected.set()
        self._channel.close()

    def _noViewsOutstanding(self):
        with self._lock:
            return not self._versioned_data._version_number_refcount

    def authenticate(self, token):
        self._channel.write(
            ClientToServer.Authenticate(token=token)
        )

    def addSchema(self, schema):
        schema.freeze()

        with self._lock:
            if schema.name not in self._schemas:
                self._schemas[schema.name] = schema

                schemaDesc = schema.toDefinition()

                self._channel.write(
                    ClientToServer.DefineSchema(
                        name=schema.name,
                        definition=schemaDesc
                    )
                )

                self._schema_response_events[schema.name] = threading.Event()

            e = self._schema_response_events[schema.name]

        e.wait()

        if self.disconnected.is_set():
            raise DisconnectedException()

    def flush(self):
        """Make sure we know all transactions that have happened up to this point."""
        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

            self._flushIx += 1
            ix = self._flushIx
            e = self._flushEvents[ix] = threading.Event()
            self._channel.write(ClientToServer.Flush(guid=ix))

        e.wait()

        if self.disconnected.is_set():
            raise DisconnectedException()

    def subscribeToObject(self, t):
        self.subscribeToObjects([t])

    def subscribeToObjects(self, objects):
        for t in objects:
            self.addSchema(type(t).__schema__)
        self.subscribeMultiple([
            (type(t).__schema__.name, type(t).__qualname__, ("_identity", keymapping.index_value_to_hash(t._identity)), False)
                for t in objects
            ])

    def _lazinessForType(self, typeObj, desiredLaziness):
        if desiredLaziness is not None:
            return desiredLaziness
        if hasattr(typeObj, '__object_database_lazy_subscription__'):
            return True
        return False

    def subscribeToIndex(self, t, block=True, lazySubscription=None, **kwarg):
        self.addSchema(t.__schema__)

        toSubscribe = []
        for fieldname, fieldvalue in kwarg.items():
            toSubscribe.append((
                t.__schema__.name,
                t.__qualname__,
                (fieldname, keymapping.index_value_to_hash(fieldvalue)),
                self._lazinessForType(t, lazySubscription)
            )
            )

        return self.subscribeMultiple(toSubscribe, block=block)

    def subscribeToType(self, t, block=True, lazySubscription=None):
        self.addSchema(t.__schema__)

        if self._isTypeSubscribedAll(t):
            return ()

        return self.subscribeMultiple([(t.__schema__.name, t.__qualname__, None, self._lazinessForType(t, lazySubscription))], block)

    def subscribeToNone(self, t, block=True):
        self.addSchema(t.__schema__)
        with self._lock:
            self._schema_and_typename_to_subscription_set.setdefault(
                (t.__schema__.name, t.__qualname__), set()
            )
        return ()

    def subscribeToSchema(self, *schemas, block=True, lazySubscription=None, excluding=()):
        for s in schemas:
            self.addSchema(s)

        unsubscribedTypes = []
        for schema in schemas:
            for tname, t in schema._types.items():
                if not self._isTypeSubscribedAll(t) and t not in excluding:
                    unsubscribedTypes.append((schema.name, tname, None, self._lazinessForType(t, lazySubscription)))

        if unsubscribedTypes:
            return self.subscribeMultiple(unsubscribedTypes, block=block)

        return ()

    def isSubscribedToSchema(self, schema):
        return all(self._isTypeSubscribed(t) for t in schema._types.values())

    def isSubscribedToType(self, t):
        return self._isTypeSubscribed(t)

    def _isTypeSubscribed(self, t):
        return (t.__schema__.name, t.__qualname__) in self._schema_and_typename_to_subscription_set

    def _isTypeSubscribedAll(self, t):
        return self._schema_and_typename_to_subscription_set.get((t.__schema__.name, t.__qualname__)) is Everything

    def subscribeMultiple(self, subscriptionTuples, block=True):
        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

            events = []

            for tup in subscriptionTuples:
                e = self._pendingSubscriptions.get(tup)

                if not e:
                    e = self._pendingSubscriptions[(tup[0], tup[1], tup[2])] = threading.Event()

                assert tup[0] and tup[1]

                self._channel.write(
                    ClientToServer.Subscribe(schema=tup[0], typename=tup[1], fieldname_and_value=tup[2], isLazy=tup[3])
                )

                events.append(e)

        if not block:
            return tuple(events)

        for e in events:
            e.wait()

        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

        return ()

    def waitForCondition(self, cond, timeout):
        # eventally we will replace this with something that watches the calculation
        t0 = time.time()
        while time.time() - t0 < timeout:
            with self.view():
                try:
                    if cond():
                        return True
                except Exception:
                    self._logger.error("Condition callback threw an exception:\n%s", traceback.format_exc())

                time.sleep(min(timeout / 20, .25))
        return False

    def _data_key_to_object(self, key):
        schema_name, typename, identity, fieldname = keymapping.split_data_key(key)

        schema = self._schemas.get(schema_name)
        if not schema:
            return None, None

        cls = schema._types.get(typename)

        if cls:
            return cls.fromIdentity(identity), fieldname

        return None, None

    def __str__(self):
        return "DatabaseConnection(%s)" % id(self)

    def __repr__(self):
        return "DatabaseConnection(%s)" % id(self)

    def current_transaction(self):
        if not hasattr(_cur_view, "view"):
            return None
        return _cur_view.view

    def view(self, transaction_id=None):
        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

            if transaction_id is None:
                transaction_id = self._cur_transaction_num

            assert transaction_id <= self._cur_transaction_num

            view = View(self, transaction_id)

            self._versioned_data.versionIncref(transaction_id)

            return view

    def transaction(self):
        """Only one transaction may be committed on the current transaction number."""
        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

            view = Transaction(self, self._cur_transaction_num)

            transaction_id = self._cur_transaction_num

            self._versioned_data.versionIncref(transaction_id)

            return view

    def _releaseView(self, view):
        with self._lock:
            self._versioned_data.versionDecref(view._transaction_num)

    def isSubscribedToObject(self, object):
        return not self._suppressKey(object._identity)

    def _suppressKey(self, k):
        fieldId = k.fieldId
        identity = k.objId

        fieldDef = self._field_id_to_field_def[fieldId]

        subscriptionSet = self._schema_and_typename_to_subscription_set.get((fieldDef.schema,fieldDef.typename))

        if subscriptionSet is Everything:
            return False

        if isinstance(subscriptionSet, set) and identity in subscriptionSet:
            return False
        return True

    def _suppressIdentities(self, index_key, identities):
        fieldDef = self._field_id_to_field_def[index_key.fieldId]

        subscriptionSet = self._schema_and_typename_to_subscription_set.get((fieldDef.schema,fieldDef.typename))

        if subscriptionSet is Everything:
            return identities
        elif subscriptionSet is None:
            return set()
        else:
            return identities.intersection(subscriptionSet)

    def cleanup(self):
        with self._lock:
            self._versioned_data.cleanup(self._cur_transaction_num)

    def _onMessage(self, msg):
        self._messages_received += 1

        if msg.matches.Disconnected:
            with self._lock:
                self.disconnected.set()
                self.connectionObject = None

                for e in self._lazy_object_read_blocks.values():
                    e.set()

                for e in self._flushEvents.values():
                    e.set()

                for e in self._pendingSubscriptions.values():
                    e.set()

                for e in self._schema_response_events.values():
                    e.set()

                for q in self._transaction_callbacks.values():
                    try:
                        q(TransactionResult.Disconnected())
                    except Exception:
                        self._logger.error(
                            "Transaction commit callback threw an exception:\n%s",
                            traceback.format_exc()
                        )

                self._transaction_callbacks = {}
                self._flushEvents = {}
        elif msg.matches.FlushResponse:
            with self._lock:
                e = self._flushEvents.get(msg.guid)
                if not e:
                    self._logger.error("Got an unrequested flush response: %s", msg.guid)
                else:
                    e.set()
        elif msg.matches.Initialize:
            with self._lock:
                self._cur_transaction_num = msg.transaction_num
                self.identityProducer = IdentityProducer(msg.identity_root)
                self.connectionObject = core_schema.Connection.fromIdentity(msg.connIdentity)
                self.initialized.set()
        elif msg.matches.TransactionResult:
            with self._lock:
                try:
                    self._transaction_callbacks.pop(msg.transaction_guid)(
                        TransactionResult.Success() if msg.success else
                        TransactionResult.RevisionConflict(key=msg.badKey)
                    )
                except Exception:
                    self._logger.error(
                        "Transaction commit callback threw an exception:\n%s",
                        traceback.format_exc()
                    )
        elif msg.matches.Transaction:
            with self._lock:
                key_value = {}
                priors = {}

                writes = {k: msg.writes[k] for k in msg.writes}
                set_adds = {k: msg.set_adds[k] for k in msg.set_adds}
                set_removes = {k: msg.set_removes[k] for k in msg.set_removes}

                for k, val_serialized in writes.items():
                    if not self._suppressKey(k):
                        key_value[k] = val_serialized

                        priors[k] = self._versioned_data.setVersionedValue(k, msg.transaction_id, val_serialized)

                for k, a in set_adds.items():
                    a = self._suppressIdentities(k, set(a))

                    self._versioned_data.setVersionedAddsAndRemoves(k, msg.transaction_id, a, set())

                for k, r in set_removes.items():
                    r = self._suppressIdentities(k, set(r))
                    self._versioned_data.setVersionedAddsAndRemoves(k, msg.transaction_id, set(), r)

                self._cur_transaction_num = msg.transaction_id

                self._versioned_data.cleanup(self._cur_transaction_num)

            for handler in self._onTransactionHandlers:
                try:
                    handler(key_value, priors, set_adds, set_removes, msg.transaction_id)
                except Exception:
                    self._logger.error(
                        "_onTransaction handler %s threw an exception:\n%s",
                        handler,
                        traceback.format_exc()
                        )

        elif msg.matches.SchemaMapping:
            with self._lock:
                for fieldDef, fieldId in msg.mapping.items():
                    self._field_id_to_field_def[fieldId] = fieldDef
                    self._fields_to_field_ids[fieldDef] = fieldId

                self._schema_response_events[msg.schema].set()

        elif msg.matches.SubscriptionIncrease:
            with self._lock:
                subscribedIdentities = self._schema_and_typename_to_subscription_set.setdefault((msg.schema, msg.typename), set())
                if subscribedIdentities is not Everything:
                    subscribedIdentities.update(
                        msg.identities
                    )
        elif msg.matches.SubscriptionData:
            with self._lock:
                lookupTuple = (msg.schema, msg.typename, msg.fieldname_and_value)

                if lookupTuple not in self._subscription_buildup:
                    self._subscription_buildup[lookupTuple] = {'values': {}, 'index_values': {}, 'identities': None, 'markedLazy': False}
                else:
                    assert not self._subscription_buildup[lookupTuple]['markedLazy'], 'received non-lazy data for a lazy subscription'

                self._subscription_buildup[lookupTuple]['values'].update({k: msg.values[k] for k in msg.values})
                self._subscription_buildup[lookupTuple]['index_values'].update({k: msg.index_values[k] for k in msg.index_values})

                if msg.identities is not None:
                    if self._subscription_buildup[lookupTuple]['identities'] is None:
                        self._subscription_buildup[lookupTuple]['identities'] = set()
                    self._subscription_buildup[lookupTuple]['identities'].update(msg.identities)
        elif msg.matches.LazyTransactionPriors:
            with self._lock:
                for k,v in msg.writes.items():
                    self._versioned_data.setVersionedTailValueStringified(k,v)
        elif msg.matches.LazyLoadResponse:
            with self._lock:
                for k,v in msg.values.items():
                    self._versioned_data.setVersionedTailValueStringified(k,v)

                self._lazy_objects.pop(msg.identity, None)

                e = self._lazy_object_read_blocks.pop(msg.identity, None)
                if e:
                    e.set()

        elif msg.matches.LazySubscriptionData:
            with self._lock:
                lookupTuple = (msg.schema, msg.typename, msg.fieldname_and_value)

                assert lookupTuple not in self._subscription_buildup

                self._subscription_buildup[lookupTuple] = {
                    'values': {},
                    'index_values': msg.index_values,
                    'identities': msg.identities,
                    'markedLazy': True
                }

        elif msg.matches.SubscriptionComplete:
            with self._lock:
                event = self._pendingSubscriptions.get((
                    msg.schema,
                    msg.typename,
                    tuple(msg.fieldname_and_value) if msg.fieldname_and_value is not None else None
                ))

                if not event:
                    self._logger.error(
                        "Received unrequested subscription to schema %s / %s / %s. have %s",
                        msg.schema, msg.typename, msg.fieldname_and_value, self._pendingSubscriptions
                    )
                    return

                lookupTuple = (msg.schema, msg.typename, msg.fieldname_and_value)

                identities = self._subscription_buildup[lookupTuple]['identities']
                values = self._subscription_buildup[lookupTuple]['values']
                index_values = self._subscription_buildup[lookupTuple]['index_values']
                markedLazy = self._subscription_buildup[lookupTuple]['markedLazy']
                del self._subscription_buildup[lookupTuple]

                sets = self.indexValuesToSetAdds(index_values)

                if msg.fieldname_and_value is None:
                    if msg.typename is None:
                        for tname in self._schemas[msg.schema]._types:
                            self._schema_and_typename_to_subscription_set[msg.schema, tname] = Everything
                    else:
                        self._schema_and_typename_to_subscription_set[msg.schema, msg.typename] = Everything
                else:
                    assert msg.typename is not None
                    subscribedIdentities = self._schema_and_typename_to_subscription_set.setdefault((msg.schema, msg.typename), set())
                    if subscribedIdentities is not Everything:
                        subscribedIdentities.update(
                            identities
                        )

                t0 = time.time()
                heartbeatInterval = getHeartbeatInterval()

                # this is a fault injection to allow us to verify that heartbeating during this
                # function will keep the server connection alive.
                for _ in range(self._largeSubscriptionHeartbeatDelay):
                    self._channel.sendMessage(
                        ClientToServer.Heartbeat()
                    )
                    time.sleep(heartbeatInterval)

                totalBytes = 0
                for k, v in values.items():
                    if v is not None:
                        totalBytes += len(v)

                if totalBytes > 1000000:
                    self._logger.info("Subscription %s loaded %.2f mb of raw data.", lookupTuple, totalBytes / 1024.0 ** 2)

                if markedLazy:
                    schema_and_typename = lookupTuple[:2]
                    for i in identities:
                        self._lazy_objects[i] = schema_and_typename

                for key, val in values.items():
                    self._versioned_data.setVersionedValue(key, msg.tid, val)

                    # this could take a long time, so we need to keep heartbeating
                    if time.time() - t0 > heartbeatInterval:
                        # note that this needs to be 'sendMessage' which sends immediately,
                        # not, 'write' which queues the message after this function finishes!
                        self._channel.sendMessage(
                            ClientToServer.Heartbeat()
                        )
                        t0 = time.time()

                for key, setval in sets.items():
                    self._versioned_data.updateVersionedAdds(key, msg.tid, set(setval))

                    # this could take a long time, so we need to keep heartbeating
                    if time.time() - t0 > heartbeatInterval:
                        # note that this needs to be 'sendMessage' which sends immediately,
                        # not, 'write' which queues the message after this function finishes!
                        self._channel.sendMessage(
                            ClientToServer.Heartbeat()
                        )
                        t0 = time.time()

                # this should be inline with the stream of messages coming from the server
                assert self._cur_transaction_num <= msg.tid

                self._cur_transaction_num = msg.tid

                event.set()
        else:
            assert False, "unknown message type " + msg._which

    def indexValuesToSetAdds(self, indexValues):
        # indexValues contains (schema:typename:identity:fieldname -> indexHashVal) which builds
        # up the indices we need. We need to transpose to a dictionary ordered by the hash values,
        # not the identities

        t0 = time.time()
        heartbeatInterval = getHeartbeatInterval()

        setAdds = {}

        for iv in indexValues:
            val = indexValues[iv]

            if val is not None:
                fieldId = iv.fieldId
                identity = iv.objId

                index_key = IndexId(fieldId=fieldId,indexValue=val)

                setAdds.setdefault(index_key, set()).add(identity)

                # this could take a long time, so we need to keep heartbeating
                if time.time() - t0 > heartbeatInterval:
                    # note that this needs to be 'sendMessage' which sends immediately,
                    # not, 'write' which queues the message after this function finishes!
                    self._channel.sendMessage(
                        ClientToServer.Heartbeat()
                    )
                    t0 = time.time()
        return setAdds

    def _get_versioned_set_data(self, key, transaction_id):
        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

            return self._versioned_data.setForVersion(key, transaction_id)

    def _get_versioned_object_data(self, key, transaction_id):
        with self._lock:
            if self._versioned_data.hasDataForKey(key):
                return self._versioned_data.valueForVersion(key, transaction_id)

            if self.disconnected.is_set():
                raise DisconnectedException()

            identity = key.objId
            if identity not in self._lazy_objects:
                return None

            event = self._loadLazyObject(identity)

        event.wait()

        with self._lock:
            if self.disconnected.is_set():
                raise DisconnectedException()

            if self._versioned_data.hasDataForKey(key):
                return self._versioned_data.valueForVersion(key, transaction_id)

            return None

    def requestLazyObjects(self, objects):
        with self._lock:
            for o in objects:
                fieldId = self._fields_to_field_ids[
                    FieldDefinition(schema=o.__schema__.name, typename=type(o).__qualname__, fieldname=" exists")
                    ]
                objFieldId = ObjectFieldId(fieldId=fieldId,objId=o._identity)

                if o._identity in self._lazy_objects and not self._versioned_data.hasDataForKey(objFieldId):
                    self._loadLazyObject(o._identity)

    def _loadLazyObject(self, identity):
        e = self._lazy_object_read_blocks.get(identity)

        if e:
            return e

        e = self._lazy_object_read_blocks[identity] = threading.Event()

        self._channel.write(
            ClientToServer.LoadLazyObject(
                identity=identity,
                schema=self._lazy_objects[identity][0],
                typename=self._lazy_objects[identity][1]
            )
        )

        return e

    def _set_versioned_object_data(self,
                                   key_value,
                                   set_adds,
                                   set_removes,
                                   keys_to_check_versions,
                                   indices_to_check_versions,
                                   as_of_version,
                                   confirmCallback
                                   ):
        assert confirmCallback is not None

        transaction_guid = self.identityProducer.createIdentity()

        self._transaction_callbacks[transaction_guid] = confirmCallback

        out_writes = {}

        for k,v in key_value.items():
            out_writes[k] = v.serializedByteRep
            if len(out_writes) > 10000:
                self._channel.write(
                    ClientToServer.TransactionData(
                        writes=out_writes, set_adds={}, set_removes={},
                        key_versions=(), index_versions=(),
                        transaction_guid=transaction_guid
                    )
                )
                self._channel.write(ClientToServer.Heartbeat())
                out_writes = {}

        ct = 0
        out_set_adds = {}
        for k, v in set_adds.items():
            out_set_adds[k] = tuple(v)
            ct += len(v)

            if len(out_set_adds) > 10000 or ct > 100000:
                self._channel.write(
                    ClientToServer.TransactionData(
                        writes={}, set_adds=out_set_adds, set_removes={},
                        key_versions=(), index_versions=(),
                        transaction_guid=transaction_guid
                    )
                )
                self._channel.write(ClientToServer.Heartbeat())
                out_set_adds = {}
                ct = 0

        ct = 0
        out_set_removes = {}
        for k, v in set_removes.items():
            out_set_removes[k] = tuple(v)
            ct += len(v)

            if len(out_set_removes) > 10000 or ct > 100000:
                self._channel.write(
                    ClientToServer.TransactionData(
                        writes={}, set_adds={}, set_removes=out_set_removes,
                        key_versions=(), index_versions=(),
                        transaction_guid=transaction_guid
                    )
                )
                self._channel.write(ClientToServer.Heartbeat())
                out_set_removes = {}
                ct = 0

        keys_to_check_versions = list(keys_to_check_versions)
        while len(keys_to_check_versions) > 10000:
            self._channel.write(
                ClientToServer.TransactionData(
                    writes={}, set_adds={}, set_removes={},
                    key_versions=keys_to_check_versions[:10000],
                    index_versions=(), transaction_guid=transaction_guid
                )
            )
            self._channel.write(ClientToServer.Heartbeat())
            keys_to_check_versions = keys_to_check_versions[10000:]

        indices_to_check_versions = list(indices_to_check_versions)
        while len(indices_to_check_versions) > 10000:
            self._channel.write(
                ClientToServer.TransactionData(
                    writes={}, set_adds={}, set_removes={},
                    key_versions=(), index_versions=indices_to_check_versions[:10000],
                    transaction_guid=transaction_guid)
            )
            indices_to_check_versions = indices_to_check_versions[10000:]

        self._channel.write(
            ClientToServer.TransactionData(
                writes=out_writes,
                set_adds=out_set_adds,
                set_removes=out_set_removes,
                key_versions=keys_to_check_versions,
                index_versions=indices_to_check_versions,
                transaction_guid=transaction_guid
            )
        )

        self._channel.write(
            ClientToServer.CompleteTransaction(
                as_of_version=as_of_version,
                transaction_guid=transaction_guid
            )
        )
