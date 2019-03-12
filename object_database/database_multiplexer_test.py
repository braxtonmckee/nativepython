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

class ObjectDatabaseMultiplexerOverChannelTests(unittest.TestCase, ObjectDatabaseMultiplexerTests):
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

    def createNewDb(self):
        multiplexer = InMemMultiplexer(self.server, auth_token=self.auth_token)
        multiplexer.start()
        self.multiplexers.append(multiplexer)
        return multiplexer.connect(self.auth_token)

    def tearDown(self):
        for multiplexer in self.multiplexers:
            multiplexer.stop()
        self.server.stop()