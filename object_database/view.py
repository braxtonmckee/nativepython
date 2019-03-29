#   Copyright 2019 Nativepython authors
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

from typed_python import serialize, deserialize

from object_database.keymapping import index_value_to_hash

import object_database
import logging
import threading
import queue
import time

LOG_SLOW_COMMIT_THRESHOLD = 1.0


class DisconnectedException(Exception):
    pass


class RevisionConflictException(Exception):
    pass


class FieldNotDefaultInitializable(Exception):
    pass


class ObjectDoesntExistException(Exception):
    def __init__(self, obj):
        super().__init__("%s(%s)" % (type(obj).__qualname__, obj._identity))
        self.obj = obj


def revisionConflictRetry(f):
    MAX_TRIES = 100

    def inner(*args, **kwargs):
        tries = 0
        while tries < MAX_TRIES:
            try:
                return f(*args, **kwargs)
            except RevisionConflictException as e:
                logging.getLogger(__name__).info(
                    "Handled a revision conflict on key %s in %s. Retrying." % (e, f.__name__)
                )
                tries += 1

        raise RevisionConflictException()

    inner.__name__ = f.__name__
    return inner


class SerializedDatabaseValue:
    """A value stored as Json with a python representation."""

    def __init__(self, serializedByteRep, pyRep):
        assert serializedByteRep is None or isinstance(serializedByteRep, bytes), serializedByteRep
        self.pyRep = pyRep
        self.serializedByteRep = serializedByteRep


def coerce_value(value, toType):
    if isinstance(value, toType):
        return value
    if hasattr(toType, "__typed_python_category__"):
        return toType(value)
    if toType in (int, float, bool):
        return toType(value)

    raise TypeError("Can't coerce %s to type %s" % (value, toType))


def default_initialize(t):
    if isinstance(None, t):
        return None
    if issubclass(t, object_database.object.DatabaseObject):
        raise FieldNotDefaultInitializable(f"Can't default initialize a {t}")
    return t()


_cur_view = threading.local()


class View(object):
    _writeable = False

    def __init__(self, db, transaction_id):
        object.__init__(self)
        self._db = db
        self._transaction_num = transaction_id
        self.serializationContext = db.serializationContext
        self._writes = {}
        self._reads = set()
        self._indexReads = set()
        self._set_adds = {}
        self._set_removes = {}
        self._t0 = None
        self._stack = None
        self._insistReadsConsistent = True
        self._insistWritesConsistent = True
        self._insistIndexReadsConsistent = False
        self._confirmCommitCallback = None
        self._objects_being_constructed = set()
        self._logger = logging.getLogger(__name__)

    def db(self):
        return self._db

    def setSerializationContext(self, serializationContext):
        self.serializationContext = serializationContext.withoutCompression()
        return self

    def transaction_id(self):
        return self._transaction_num

    def getFieldIdFor(self, cls, identity, fieldname, isIndex=False):
        fieldId = self._db._fields_to_field_ids[
            object_database.schema.FieldDefinition(
                schema=cls.__schema__.name,
                typename=cls.__qualname__,
                fieldname=fieldname
            )
        ]
        return object_database.schema.ObjectFieldId(objId=identity, fieldId=fieldId)

    def getIndexId(self, cls, fieldname, indexValue):
        key = object_database.schema.FieldDefinition(
            schema=cls.__schema__.name,
            typename=cls.__qualname__,
            fieldname=fieldname
        )

        fieldId = self._db._fields_to_field_ids.get(key)

        return object_database.schema.IndexId(
            fieldId=fieldId,
            indexValue=index_value_to_hash(indexValue, self.serializationContext)
        )

    def _new(self, cls, args, kwds):
        if not self._writeable:
            raise Exception("Views are static. Please open a transaction.")

        if not self._db._isTypeSubscribed(cls):
            raise Exception("No subscriptions exist for type %s" % cls)

        if "_identity" in kwds and not args and len(kwds) == 1:
            identity = kwds["_identity"]
            kwds = dict(kwds)
            del kwds["_identity"]
        else:
            identity = self._db.identityProducer.createIdentity()

        o = cls.fromIdentity(identity)

        try:
            self._writes[self.getFieldIdFor(cls, identity, " exists")] = (bool, True)

            if not hasattr(cls, '__types__') or cls.__types__ is None:
                raise Exception("Please initialize the type object for %s" % str(cls.__qualname__))

            if hasattr(cls, '__odb_initializer__'):
                self._add_to_index(self.getIndexId(cls, " exists", True), identity)

                cls.__odb_initializer__(o, *args, **kwds)

                for tname, t in cls.__types__.items():
                    k = self.getFieldIdFor(cls, identity, tname)
                    if k not in self._writes:
                        setattr(o, tname, default_initialize(t))
            else:
                if args:
                    raise Exception(f"Default constructor for {cls.__qualname__} doesn't accept positional arguments.")

                self._defaultConstructInstance(cls, o, kwds)

        except Exception:
            # make sure we leave no trace of this object.
            for tname in cls.__types__:
                k = self.getFieldIdFor(cls, identity, tname)
                if k in self._writes:
                    del self._writes[k]

            del self._writes[self.getFieldIdFor(cls, identity, " exists")]

            # propagate the exception
            raise

        return o

    def _defaultConstructInstance(self, cls, o, kwds):
        kwds = dict(kwds)

        identity = o._identity

        for tname, t in cls.__types__.items():
            if tname not in kwds:
                kwds[tname] = default_initialize(t)

        for kwd, val in kwds.items():
            if kwd not in cls.__types__:
                raise TypeError("Unknown field %s on %s" % (kwd, cls))

            targetType = cls.__types__[kwd]

            try:
                coerced_val = coerce_value(val, targetType)
            except Exception:
                raise TypeError(f"Can't coerce {val} to type {targetType} when initializing {kwd} of {cls.__qualname__}")

            self._writes[self.getFieldIdFor(cls, identity, kwd)] = (cls.__types__[kwd], coerced_val)

        if cls in cls.__schema__._indices:
            for index_name, index_fun in cls.__schema__._indices[cls].items():
                try:
                    val = index_fun(o)
                    if val is not None:
                        indexType = cls.__schema__._indexTypes[cls][index_name]
                        ik = self.getIndexId(cls, index_name, coerce_value(val, indexType))

                        self._add_to_index(ik, identity)

                except FieldNotDefaultInitializable:
                    pass

    def _get(self, obj, identity, field_name, field_type):
        key = self.getFieldIdFor(type(obj), identity, field_name)

        self._reads.add(key)

        if key in self._writes:
            if self._writes[key] is None:
                if not self._db._isTypeSubscribed(type(obj)):
                    raise Exception("No subscriptions exist for type %s" % obj)

                if not obj.exists():
                    raise ObjectDoesntExistException(obj)

            res = self._writes[key][1]

            if isinstance(res, (tuple, list)):
                return res[1]

            return res

        dbValWithPyrep = self._db._get_versioned_object_data(key, self._transaction_num)

        if dbValWithPyrep is None:
            if not self._db._isTypeSubscribed(type(obj)):
                raise Exception("No subscriptions exist for type %s" % obj)

            if not obj.exists():
                raise ObjectDoesntExistException(obj)

        return self.unwrapSerializedDatabaseValue(self.serializationContext, dbValWithPyrep, field_type)

    @staticmethod
    def unwrapSerializedDatabaseValue(serializationContext, dbValWithPyrep, field_type):
        assert field_type is not None

        if dbValWithPyrep is None:
            return default_initialize(field_type)

        if isinstance(dbValWithPyrep, bytes):
            dbValWithPyrep = SerializedDatabaseValue(dbValWithPyrep, {})

        if dbValWithPyrep.serializedByteRep is None:
            return default_initialize(field_type)

        if dbValWithPyrep.pyRep.get(serializationContext) is None:
            dbValWithPyrep.pyRep[serializationContext] = deserialize(field_type, dbValWithPyrep.serializedByteRep, serializationContext)

        return dbValWithPyrep.pyRep[serializationContext]

    def _exists(self, obj, identity):
        if not self._db._isTypeSubscribed(type(obj)):
            raise Exception("No subscriptions exist for type %s" % obj)

        key = self.getFieldIdFor(type(obj), identity, " exists")

        self._reads.add(key)

        if key in self._writes:
            return self._writes[key]

        val = self._db._get_versioned_object_data(key, self._transaction_num)

        return val is not None and val.serializedByteRep is not None

    def _delete(self, obj, identity, field_names):
        if not self._db._isTypeSubscribed(type(obj)):
            raise Exception("No subscriptions exist for type %s" % obj)

        if not obj.exists():
            raise ObjectDoesntExistException(obj)

        if hasattr(type(obj), '__odb_destructor__'):
            type(obj).__odb_destructor__(obj)

        existing_index_vals = self._compute_index_vals(obj, obj.__schema__._indices.get(type(obj), {}).keys())

        for name in field_names:
            key = self.getFieldIdFor(type(obj), identity, name)
            self._writes[key] = None

        self._writes[self.getFieldIdFor(type(obj), identity, " exists")] = None

        self._update_indices(obj, identity, existing_index_vals, {})

    def _set(self, obj, identity, field_name, field_type, val):
        if not self._db._isTypeSubscribed(type(obj)):
            raise Exception("No subscriptions exist for type %s" % obj)

        if not self._writeable:
            raise Exception("Views are static. Please open a transaction.")

        if not obj.exists():
            raise ObjectDoesntExistException(obj)

        key = self.getFieldIdFor(type(obj), identity, field_name)

        indicesChanged = obj.__schema__._indexed_fields[type(obj)].get(field_name, [])

        if not indicesChanged:
            self._writes[key] = (field_type, val)
        else:
            existing_index_vals = self._compute_index_vals(obj, indicesChanged)

            self._writes[key] = (field_type, val)

            new_index_vals = self._compute_index_vals(obj, indicesChanged)

            self._update_indices(obj, identity, existing_index_vals, new_index_vals)

    def _compute_index_vals(self, obj, indicesChanged):
        existing_index_vals = {}

        for index_name in indicesChanged:
            index_fun = obj.__schema__._indices[type(obj)][index_name]
            indexType = obj.__schema__._indexTypes[type(obj)][index_name]

            try:
                unconvertedVal = index_fun(obj)

                if unconvertedVal is not None and indexType is None:
                    assert False, (obj, index_name, unconvertedVal)

                existing_index_vals[index_name] = coerce_value(unconvertedVal, indexType) if unconvertedVal is not None else None
            except FieldNotDefaultInitializable:
                pass

        return existing_index_vals

    def _update_indices(self, obj, identity, existing_index_vals, new_index_vals):
        if type(obj) in obj.__schema__._indices:
            for index_name in obj.__schema__._indices[type(obj)]:
                new_index_val = new_index_vals.get(index_name, None)
                cur_index_val = existing_index_vals.get(index_name, None)

                if cur_index_val != new_index_val:
                    if cur_index_val is not None:
                        old_index_name = self.getIndexId(type(obj), index_name, cur_index_val)
                        self._remove_from_index(old_index_name, identity)

                    if new_index_val is not None:
                        new_index_name = self.getIndexId(type(obj), index_name, new_index_val)
                        self._add_to_index(new_index_name, identity)

    def _add_to_index(self, index_key, identity):
        if index_key not in self._set_adds:
            self._set_adds[index_key] = set()
            self._set_removes[index_key] = set()

        if identity in self._set_removes[index_key]:
            self._set_removes[index_key].discard(identity)
        else:
            self._set_adds[index_key].add(identity)

    def _remove_from_index(self, index_key, identity):
        if index_key not in self._set_adds:
            self._set_adds[index_key] = set()
            self._set_removes[index_key] = set()

        if identity in self._set_adds[index_key]:
            self._set_adds[index_key].discard(identity)
        else:
            self._set_removes[index_key].add(identity)

    def indexLookup(self, db_type, **kwargs):
        if not self._db._isTypeSubscribed(db_type):
            raise Exception("No subscriptions exist for type %s" % db_type)

        assert len(kwargs) == 1, "Can only lookup one index at a time."
        tname, value = list(kwargs.items())[0]

        if db_type not in db_type.__schema__._indices or tname not in db_type.__schema__._indices[db_type]:
            raise Exception("No index enabled for %s.%s.%s" % (db_type.__schema__.name, db_type.__qualname__, tname))

        if not hasattr(_cur_view, "view"):
            raise Exception("Please access indices from within a view.")

        indexType = db_type.__schema__._indexTypes[db_type][tname]

        if indexType is not None:
            value = coerce_value(value, indexType)

        keyname = self.getIndexId(db_type, tname, value)

        self._indexReads.add(keyname)

        identities = self._db._get_versioned_set_data(keyname, self._transaction_num).toSet()
        identities = identities.union(self._set_adds.get(keyname, set()))
        identities = identities.difference(self._set_removes.get(keyname, set()))

        return tuple([db_type.fromIdentity(x) for x in identities])

    def indexLookupAny(self, db_type, **kwargs):
        if not self._db._isTypeSubscribed(db_type):
            raise Exception("No subscriptions exist for type %s" % db_type)

        assert len(kwargs) == 1, "Can only lookup one index at a time."
        tname, value = list(kwargs.items())[0]

        if db_type not in db_type.__schema__._indices or tname not in db_type.__schema__._indices[db_type]:
            raise Exception("No index enabled for %s.%s.%s" % (db_type.__schema__.name, db_type.__qualname__, tname))

        if not hasattr(_cur_view, "view"):
            raise Exception("Please access indices from within a view.")

        indexType = db_type.__schema__._indexTypes[db_type][tname]

        if indexType is not None:
            value = coerce_value(value, indexType)

        keyname = self.getIndexId(db_type, tname, value)

        added = self._set_adds.get(keyname, set())
        removed = self._set_removes.get(keyname, set())

        if added:
            return db_type.fromIdentity(list(added)[0])

        self._indexReads.add(keyname)

        res = self._db._get_versioned_set_data(keyname, self._transaction_num).pickAny(removed)

        if res:
            return db_type.fromIdentity(res)

        return None

    def indexLookupOne(self, lookup_type, **kwargs):
        if not self._db._isTypeSubscribed(lookup_type):
            raise Exception("No subscriptions exist for type %s" % lookup_type)

        res = self.indexLookup(lookup_type, **kwargs)
        if not res:
            raise Exception("No instances of %s found with %s" % (lookup_type, kwargs))
        if len(res) != 1:
            raise Exception("Multiple instances of %s found with %s" % (lookup_type, kwargs))
        return res[0]

    def commit(self):
        if not self._writeable:
            raise Exception("Views are static. Please open a transaction.")

        if self._writes:
            def encode(val):
                if isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], type):
                    serializedBytes = serialize(val[0], val[1], self.serializationContext)
                    return SerializedDatabaseValue(
                        serializedBytes,
                        {self.serializationContext: val[1]}
                    )

                elif val is None:
                    return SerializedDatabaseValue(val, {})
                else:
                    assert False, "bad write: %s" % val

            writes = {key: encode(v) for key, v in self._writes.items()}
            tid = self._transaction_num

            if (self._set_adds or self._set_removes) and not self._insistReadsConsistent:
                raise Exception("You can't update an indexed value without read and write consistency.")

            if self._confirmCommitCallback is None:
                result_queue = queue.Queue()

                confirmCallback = result_queue.put
            else:
                confirmCallback = self._confirmCommitCallback

            self._db._set_versioned_object_data(
                writes,
                {k: v for k, v in self._set_adds.items() if v},
                {k: v for k, v in self._set_removes.items() if v},
                (
                    self._reads.union(set(writes)) if self._insistReadsConsistent else
                    set(writes) if self._insistWritesConsistent else
                    set()
                ),
                self._indexReads if self._insistIndexReadsConsistent else set(),
                tid,
                confirmCallback
            )

            if not self._confirmCommitCallback:
                # this is the synchronous case - we want to wait for the confirm
                t0 = time.time()

                res = result_queue.get()

                if time.time() - t0 > LOG_SLOW_COMMIT_THRESHOLD:
                    self._logger.info(
                        "Committing %s writes and %s set changes took %.1f seconds",
                        len(self._writes), len(self._set_adds) + len(self._set_removes), time.time() - t0
                    )

                if res.matches.Success:
                    return
                if res.matches.Disconnected:
                    raise DisconnectedException()
                if res.matches.RevisionConflict:
                    raise RevisionConflictException(res.key)

                assert False, "unknown transaction result: " + str(res)

    def nocommit(self):
        class Scope:
            def __enter__(scope):
                assert not hasattr(_cur_view, 'view')
                _cur_view.view = self

            def __exit__(self, *args):
                del _cur_view.view

        return Scope()

    def __enter__(self):
        assert not hasattr(_cur_view, 'view')
        _cur_view.view = self
        return self

    def __exit__(self, type, val, tb):
        del _cur_view.view
        try:
            if type is None and self._writes:
                self.commit()
        finally:
            self._db._releaseView(self)


class Transaction(View):
    _writeable = True

    def consistency(self, writes=False, reads=False, full=False, none=False):
        """Set the consistency model for the Transaction.

        if 'none', then the transaction always succeeds
        if 'writes', then we insist that any key you write to has not been updated, but
            allow read keys to have been updated.
        if 'reads', then we insist that any key you read or write is not updated, but allow
            index updates
        if 'full' is True, then we insist that any index you read from is not updated.
            (this is very stringent)

        This function modifies the view, and the semantics are only in place at
        commit time.
        """
        assert sum(int(i) for i in (reads, writes, full, none)) == 1, "Please set exactly one option"

        self._insistReadsConsistent = bool(reads or full)
        self._insistWritesConsistent = bool(writes or reads or full)
        self._insistIndexReadsConsistent = bool(full)

        return self

    def hasFullConsistency(self):
        return self._insistIndexReadsConsistent

    def onConfirmed(self, callback):
        """Set a callback function to be called on the main event thread with a boolean indicating
        whether the transaction was accepted."""
        self._confirmCommitCallback = callback

        return self

    def noconfirm(self):
        """Indicate that the transaction should return immediately without a round-trip to
        confirm that it was successful."""
        def ignoreConfirmResult(result):
            pass
        self._confirmCommitCallback = ignoreConfirmResult

        return self


def current_transaction():
    if not hasattr(_cur_view, "view"):
        return None
    return _cur_view.view
