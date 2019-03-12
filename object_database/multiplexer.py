from object_database.server import Server
from object_database.database_connection import DatabaseConnection, ManyVersionedObjects
from object_database.messages import ClientToServer, ServerToClient, getHeartbeatInterval
from object_database.inmem_server import InMemoryChannel
from object_database.persistence import InMemoryPersistence
from object_database.identity import IdentityProducer
from typed_python.Codebase import Codebase as TypedPythonCodebase

import time
import queue
import logging
import threading
import traceback

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
        self.msg_list = []


    def onClientToServerMessage(self, connectedChannel, msg):
        self.server_channel.write(msg)
        # I'm not sure this is right.
        if msg.matches.Flush:
            with self._lock:
                self.server_channel.write(msg)
                connectedChannel.channel.write(ServerToClient.FlushResponse(guid=msg.guid))

    def serverHandler(self, msg):
        for channel in self.channels:
            channel.write(msg)
    # def onClientToServerMessage(self, connectedChannel, msg):
    #     assert isinstance(msg, ClientToServer)
    #     # print("Client to server message: {} from multiplexer {}".format(msg, id(self)))
    #     if msg.matches.Flush:
    #         with self._lock:
    #             self.server_channel.write(msg)
    #             connectedChannel.channel.write(ServerToClient.FlushResponse(guid=msg.guid))
    #     else:
    #         self.server_channel.write(msg)

    #     if msg.matches.Subscribe:
    #         schema, typename = msg.schema, msg.typename
    #         if (schema, typename) not in self._type_to_channel:
    #             self._type_to_channel[schema, typename] = set()

    #         self._type_to_channel[schema, typename].add(connectedChannel)
    #         connectedChannel.subscribedTypes.add((schema, typename))
    #     # Server.onClientToServerMessage(self, channel, msg)

    # def serverHandler(self, msg):
    #     # Right now, send the message to every client.
    #     # In the future, we only want to send it to the appropriate ones.
    #     # print("Server to client message: {} from multiplexer {}".format(msg, id(self)))
    #     channelsTriggered = set()
    #     schemaTypePairsWriting = set()
    #     key_value = {}
    #     set_adds = {}
    #     set_removes = {}

    #     if msg.matches.Initialize:
    #         identity = msg.
    #         key_value = keymapping.data_key(core_schema.Connection, identity, " exists")

    #     for key in key_value:
    #         schema_name, typename, ident = keymapping.split_data_key(key)[:3]
    #         schemaTypePairsWriting.add((schema_name, typename))

    #     for subset in [set_adds, set_removes]:
    #         for k in subset:
    #             if subset[k]:
    #                 schema_name, typename = keymapping.split_index_key(k)[:2]

    #                 schemaTypePairsWriting.add((schema_name, typename))

    #                 setsWritingTo.add(k)

    #                 identities_mentioned.update(subset[k])

    #     for schema_type_pair in schemaTypePairsWriting:
    #         for channel in self._type_to_channel.get(schema_type_pair, ()):
    #             channelsTriggered.add(channel)

    #     for i in identities_mentioned:
    #         if i in self._id_to_channel:
    #             channelsTriggered.update(self._id_to_channel[i])

    #     for channel in channelsTriggered:
    #         channel.write(msg)

    def allocateNewIdentityRoot(self):
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
        self.server_channel = self.server.getChannel()
        self.server_channel.setServerToClientHandler(self.serverHandler)

    def stop(self):
        Server.stop(self)

        self.stopped.set()

        for c in self.channels:
            c.stop()

        self.checkForDeadConnectionsLoopThread.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, t, v, traceback):
        self.stop()

class InMemMultiplexer(Multiplexer):
    def getChannel(self):
        channel = InMemoryChannel(self)
        channel.start()

        self.addConnection(channel)
        self.channels.append(channel)
        return channel