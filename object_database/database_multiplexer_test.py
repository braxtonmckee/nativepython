from typed_python import Alternative, OneOf, ConstDict

from object_database.schema import Indexed, Index, Schema
from object_database.core_schema import core_schema
from object_database.view import RevisionConflictException, DisconnectedException, ObjectDoesntExistException
from object_database.database_connection import TransactionListener, DatabaseConnection, SetWithEdits
from object_database.tcp_server import TcpServer
from object_database.inmem_server import InMemServer
from object_database.persistence import InMemoryPersistence, RedisPersistence
from object_database.util import configureLogging, genToken
from object_database.test_util import currentMemUsageMb
from object_database.multiplexer import InMemMultiplexer
from object_database.database_test import ObjectDatabaseTests

import object_database.messages as messages
import queue
import unittest
import tempfile
import numpy
import redis
import subprocess
import os
import threading
import random
import time
import ssl

expr = Alternative(
    "Expr",
    Constant={'value': int},
    # Add = {'l': expr, 'r': expr},
    # Sub = {'l': expr, 'r': expr},
    # Mul = {'l': expr, 'r': expr}
)

schema = Schema("test_schema")
schema.expr = expr


@schema.define
class Root:
    obj = OneOf(None, schema.Object)
    k = int


@schema.define
class Object:
    k = Indexed(expr)
    other = OneOf(None, schema.Object)

    @property
    def otherK(self):
        if self.other is not None:
            return self.other.k


@schema.define
class ThingWithDicts:
    x = ConstDict(str, bytes)


@schema.define
class Counter:
    k = Indexed(int)
    x = int

    def f(self):
        return self.k + 1

    def __str__(self):
        return "Counter(k=%s)" % self.k


@schema.define
class StringIndexed:
    name = Indexed(str)

class ObjectDatabaseMultiplexerTests(ObjectDatabaseTests):
    def test_disconnecting(self):
        pass

    def test_disconnecting_is_immediate(self):
        pass

    def test_flush_db_works(self):
        return

    def test_object_versions_robust(self):
        return

class ObjectDatabaseMultiplexerOverChannelTests(ObjectDatabaseMultiplexerTests):
    @classmethod
    def setUpClass(cls):
        ObjectDatabaseTests.setUpClass()

    def setUp(self):
        self.auth_token = genToken()

        self.mem_store = InMemoryPersistence()
        self.server = InMemServer(self.mem_store, self.auth_token)
        self.server._gc_interval = .1
        self.server.start()
        self.multiplexers = []
        self._multiplexer = None

    def createNewDbFromMultiplexer(self, multiplexer):
        return multiplexer.connect(self.auth_token)

    def createNewMultiplexer(self):
        multiplexer = InMemMultiplexer(self.server, auth_token=self.auth_token)
        multiplexer.start()
        self.multiplexers.append(multiplexer)
        return multiplexer

    def tearDown(self):
        for multiplexer in self.multiplexers:
            multiplexer.stop()
        self.server.stop()

class ODbMOverChannelTestsMM(ObjectDatabaseMultiplexerOverChannelTests, unittest.TestCase):
    def createNewDb(self):
        multiplexer = self.createNewMultiplexer()
        return self.createNewDbFromMultiplexer(multiplexer)

    # def setUp(self):
    #     ObjectDatabaseMultiplexerOverChannelTests.setUp(self)

    def test_two_dbs(self):
        m = self.createNewMultiplexer()
        db1 = self.createNewDbFromMultiplexer(m)
        db2 = self.createNewDbFromMultiplexer(m)

        db1.subscribeToIndex(Counter, k=0)
        db2.subscribeToIndex(Counter, k=1)

    def test_two_multiplexers(self):
        m1 = self.createNewMultiplexer()
        m2 = self.createNewMultiplexer()
        db1 = self.createNewDbFromMultiplexer(m1)
        db2 = self.createNewDbFromMultiplexer(m2)

        db1.subscribeToSchema(schema)
        db1.subscribeToIndex(Counter, k=0)
        db2.subscribeToIndex(Counter, k=1)

    def test_three_dbs_on_two_multiplexers(self):
        m1 = self.createNewMultiplexer()
        m2 = self.createNewMultiplexer()
        db1 = self.createNewDbFromMultiplexer(m1)
        db2 = self.createNewDbFromMultiplexer(m2)
        db3 = self.createNewDbFromMultiplexer(m2)

        db1.subscribeToIndex(Counter, k=0)
        db2.subscribeToIndex(Counter, k=0)
        db3.subscribeToIndex(Counter, k=0)

    def test_wait_for_condition_one_multiplexer(self):
        m1=self.createNewMultiplexer()
        m2=self.createNewMultiplexer()
        db1 = self.createNewDbFromMultiplexer(m1)
        db2 = self.createNewDbFromMultiplexer(m1)

        db1.subscribeToSchema(schema)

        db1.subscribeToIndex(Counter, k=0)
        db2.subscribeToIndex(Counter, k=1)

        # create a new value in the view and verify it shows up
        with db1.transaction():
            c2_0 = Counter(k=0)

        # now move c2_0 from '0' to '1'. It should show up in db2 and still in db1
        with db1.transaction():
            c2_0.k = 1

        # with db1.transaction():
        #    c2_0.x = 40

        db2.waitForCondition(lambda: c2_0.x == 40, 2*self.PERFORMANCE_FACTOR)

    def test_index_subscriptions_two_multiplexers(self):
        m1=self.createNewMultiplexer()
        m2=self.createNewMultiplexer()
        db_all = self.createNewDbFromMultiplexer(m1)
        db1 = self.createNewDbFromMultiplexer(m1)
        db2 = self.createNewDbFromMultiplexer(m1)

        db_all.subscribeToSchema(schema)
        with db_all.transaction():
            c0 = Counter(k=0)
            c1 = Counter(k=1)

            c0.x = 20
            c1.x = 30

        db1.subscribeToIndex(Counter, k=0)
        db2.subscribeToIndex(Counter, k=1)

        with db1.view():
            self.assertTrue(c0.exists())
            self.assertEqual(c0.x, 20)
            self.assertFalse(c1.exists())

        with db2.view():
            self.assertTrue(c1.exists())
            self.assertEqual(c1.x, 30)
            self.assertFalse(c0.exists())

        # create a new value in the view and verify it shows up
        with db_all.transaction():
            c2_0 = Counter(k=0)
            c2_1 = Counter(k=1)

        db1.waitForCondition(lambda: c2_0.exists(), 2*self.PERFORMANCE_FACTOR)
        db2.waitForCondition(lambda: c2_1.exists(), 2*self.PERFORMANCE_FACTOR)

        with db2.view():
            self.assertFalse(c2_0.exists())
        with db1.view():
            self.assertFalse(c2_1.exists())

        # now move c2_0 from '0' to '1'. It should show up in db2 and still in db1
        with db_all.transaction():
            c2_0.k = 1

        db1.waitForCondition(lambda: c2_0.exists(), 2*self.PERFORMANCE_FACTOR)
        db2.waitForCondition(lambda: c2_0.exists(), 2*self.PERFORMANCE_FACTOR)

        # now, we should see it get subscribed to in both
        with db_all.transaction():
            c2_0.x = 40

        db1.waitForCondition(lambda: c2_0.x == 40, 2*self.PERFORMANCE_FACTOR)
        db2.waitForCondition(lambda: c2_0.x == 40, 2*self.PERFORMANCE_FACTOR)

        # but if we make a new database connection and subscribe, we won't see it
        db3 = self.createNewDbFromMultiplexer(m1)
        db3.subscribeToIndex(Counter, k=0)
        db3.flush()

        with db3.view():
            self.assertTrue(not c2_0.exists())
            self.assertTrue(not c2_1.exists())

        db4 = self.createNewDbFromMultiplexer(m2)
        db4.subscribeToIndex(Counter, k=0)
        db4.flush()

        with db4.view():
            self.assertTrue(not c2_0.exists())
            self.assertTrue(not c2_1.exists())

class ObjectDatabaseMultiplexerOverChannelTestsOneMultiplexer(ObjectDatabaseMultiplexerOverChannelTests, unittest.TestCase):
    def createNewDb(self):
        if self._multiplexer is None:
            self._multiplexer = self.createNewMultiplexer()
        return self.createNewDbFromMultiplexer(self._multiplexer)