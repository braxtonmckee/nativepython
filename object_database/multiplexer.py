from object_database.server import Server
from object_database.database_connection import DatabaseConnection, ManyVersionedObjects
from object_database.messages import ClientToServer, ServerToClient, getHeartbeatInterval, SchemaDefinition
from object_database.inmem_server import InMemoryChannel
from object_database.persistence import InMemoryPersistence
from object_database.identity import IdentityProducer
from object_database.core_schema import core_schema
from typed_python.Codebase import Codebase as TypedPythonCodebase
import object_database.keymapping as keymapping

import time
import queue
import logging
import threading
import traceback

# TODO:
# Make authentication better.
# Handle dropped connections from clients.
# Handle heartbeats better.
# Handle LazyTransactionsPrior.

class Multiplexer(Server):
    def __init__(self, server, kvstore=None, auth_token=''):
        self.server = server
        Server.__init__(self, kvstore or InMemoryPersistence(), auth_token)
        self.identityProducer = IdentityProducer(self.allocateNewIdentityRoot())
        self.channels = []
        self.stopped = threading.Event()
        self.auth_token = auth_token
        self.checkForDeadConnectionsLoopThread = threading.Thread(target=self.checkForDeadConnectionsLoop)
        self.checkForDeadConnectionsLoopThread.daemon = True
        self.checkForDeadConnectionsLoopThread.start()
        self.auth_token = auth_token

        self._guid_to_channel = {}

    def _hashGuid(self, connectedChannel, msg):
        with self._lock:
            self._guid_to_channel[msg.channel_guid] = connectedChannel

    def onClientToServerMessage(self, connectedChannel, msg):
        assert isinstance(msg, ClientToServer)

        with self._lock:
            self._updateChannelsTriggered(connectedChannel, msg)

        # Handle Authentication messages
        if msg.matches.Authenticate:
            return

        # Abort if connection is not authenticated
        # if connectedChannel.needsAuthentication:
        #     self._logger.info(
        #         "Received unexpected client message on unauthenticated channel %s",
        #         connectedChannel.connectionObject._identity
        #     )
        #     pass

        # Handle remaining types of messages
        if msg.matches.Heartbeat:
            # I'm not sure if this is entirely right.
            self.server_channel.write(msg)
            connectedChannel.heartbeat()
            return

        if msg.matches.LoadLazyObject:
            self._hashGuid(connectedChannel, msg)

        elif msg.matches.Flush:
            self._hashGuid(connectedChannel, msg)

        elif msg.matches.Subscribe:
            self._hashGuid(connectedChannel, msg)

        elif msg.matches.TransactionData:
            self._hashGuid(connectedChannel, msg)

        elif msg.matches.CompleteTransaction:
            self._hashGuid(connectedChannel, msg)


        with self._lock:
            self.server_channel.write(msg)

    def passMessage(self, msg):
        assert msg.channel_guid in self._guid_to_channel
        channel=self._guid_to_channel[msg.channel_guid]
        if channel is not None:
            channel.channel.write(msg)

    def serverHandler(self, msg):
        # print("Server to client message: {} from multiplexer {}".format(msg, id(self)))
        # channelsTriggered = set()
        # schemaTypePairsWriting = set()
        # key_value = {}
        # set_adds = {}
        # set_removes = {}

        if msg.matches.Initialize:
            return

        elif msg.matches.TransactionResult:
            self.passMessage(msg)
            return

        elif msg.matches.FlushResponse:
            self.passMessage(msg)
            return

        elif msg.matches.LazyTransactionPriors:
            # I can fix this when I understand what LazyTransactionPriors actually does.
            return

        elif msg.matches.LazyLoadResponse:
            self.passMessage(msg)
            return

        elif msg.matches.SubscriptionData:
            self.passMessage(msg)
            return

        elif msg.matches.LazySubscriptionData:
            self.passMessage(msg)
            return

        elif msg.matches.SubscriptionComplete:
            self.passMessage(msg)
            return

        elif msg.matches.SubscriptionIncrease:
            schema_name = msg.schema
            typename = msg.typename
            field, val = msg.fieldname_and_value
            index_key = keymapping.index_key_from_names_encoded(schema_name, typename, field, val)
            newIds = msg.identities

            for channel in self._index_to_channel.get(index_key, {}):
                for new_id in newIds:
                    self._id_to_channel.setdefault(new_id, set()).add(channel)
                    channel.subscribedIds.add(new_id)
                    channel.channel.write(msg)

        elif msg.matches.Disconnected:
            self.stop()

        elif msg.matches.Transaction:
            key_value = msg.writes
            set_adds = msg.set_adds
            set_removes = msg.set_removes

            channelsTriggered = self._findChannelsTriggeredTransaction(key_value, set_adds, set_removes)
            for channel in channelsTriggered:
                channel.sendTransaction(msg)

        # for channel in self.channels:
        #     channel.write(msg)

    def allocateNewIdentityRoot(self):
        with self._lock:
            return self.server.allocateNewIdentityRoot()

    def connect(self, auth_token):
        dbc = DatabaseConnection(self.getChannel())
        dbc.authenticate(auth_token)
        dbc.initialized.wait()
        return dbc

    def checkForDeadConnectionsLoop(self):
        lastCheck = time.time()
        while not self.stopped.is_set():
            if time.time() - lastCheck > getHeartbeatInterval():
                self.checkForDeadConnections()
                lastCheck = time.time()
            else:
                self.stopped.wait(0.1)

    def start(self):
        Server.start(self)
        # Remember that I'm assuming that server.connect returns a channel, not a dbc.
        self.server_channel = self.server.getChannel()[0]
        self.server_channel.setServerToClientHandler(self.serverHandler)
        self.server_channel.write(ClientToServer.Authenticate(token=self.auth_token))

    def stop(self):
        Server.stop(self)

        self.stopped.set()

        for c, _ in self.channels:
            c.stop()

        self.checkForDeadConnectionsLoopThread.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, t, v, traceback):
        self.stop()

class InMemMultiplexer(Multiplexer):
    def getChannel(self):
        guid = self.identityProducer.createIdentity()
        channel = InMemoryChannel(self)
        channel.start()

        self.addConnection(channel)
        self.channels.append((channel, guid))
        self.server_channel.write(ClientToServer.AddChannel(channel_guid=guid))
        return channel, guid

    # I should also deal with dropped connections from clients.