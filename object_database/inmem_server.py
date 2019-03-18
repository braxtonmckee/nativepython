from object_database.server import Server
from object_database.database_connection import DatabaseConnection
from object_database.messages import ClientToServer, ServerToClient, getHeartbeatInterval
from object_database.persistence import InMemoryPersistence

import time
import queue
import logging
import threading
import traceback


class InMemoryChannel:
    def __init__(self, server):
        self._server = server
        self._clientCallback = None
        self._serverCallback = None
        self._clientToServerMsgQueue = queue.Queue()
        self._serverToClientMsgQueue = queue.Queue()
        self._shouldStop = True

        self._pumpThreadServer = threading.Thread(target=self.pumpMessagesFromServer)
        self._pumpThreadServer.daemon = True

        self._pumpThreadClient = threading.Thread(target=self.pumpMessagesFromClient)
        self._pumpThreadClient.daemon = True

        self._stopHeartbeatingSet = False

        self._logger = logging.getLogger(__name__)

    def _stopHeartbeating(self):
        self._stopHeartbeatingSet = True

    def close(self):
        self.stop(block=False)
        self._clientCallback(ServerToClient.Disconnected())

    def pumpMessagesFromServer(self):
        while not self._shouldStop:
            try:
                e = self._serverToClientMsgQueue.get(timeout=1.0)
            except queue.Empty:
                e = None

            if e:
                try:
                    self._clientCallback(e)
                except Exception:
                    self._logger.error("Pump thread failed for %s: %s", self, traceback.format_exc())
                    return

    def pumpMessagesFromClient(self):
        lastHeartbeat = time.time()
        while not self._shouldStop:
            if time.time() - lastHeartbeat > getHeartbeatInterval() and not self._stopHeartbeatingSet:
                lastHeartbeat = time.time()
                e = ClientToServer.Heartbeat()
            else:
                try:
                    e = self._clientToServerMsgQueue.get(timeout=1.0)
                except queue.Empty:
                    e = None

            if e:
                try:
                    self._serverCallback(e)
                except Exception:
                    traceback.print_exc()
                    self._logger.error("Pump thread failed: %s", traceback.format_exc())
                    return

        self._server.dropConnection(self)

    def start(self):
        assert self._shouldStop
        self._shouldStop = False

    def stop(self, block=False):
        self._shouldStop = True
        self._clientToServerMsgQueue.put(None)
        self._serverToClientMsgQueue.put(None)
        if block:
            self._pumpThreadServer.join()
            self._pumpThreadClient.join()

    def sendMessage(self, msg):
        self.write(msg)

    def write(self, msg):
        # print("Channel: {}".format(id(self)))
        # print(msg)
        if isinstance(msg, ClientToServer):
            self._clientToServerMsgQueue.put(msg)
        elif isinstance(msg, ServerToClient):
            self._serverToClientMsgQueue.put(msg)
        else:
            assert False

    def setServerToClientHandler(self, callback):
        assert not self._shouldStop

        self._clientCallback = callback
        self._pumpThreadServer.start()

    def setClientToServerHandler(self, callback):
        assert not self._shouldStop

        self._serverCallback = callback
        self._pumpThreadClient.start()


class InMemServer(Server):
    def __init__(self, kvstore=None, auth_token=''):
        Server.__init__(self, kvstore or InMemoryPersistence(), auth_token)
        self.channels = []
        self.stopped = threading.Event()
        self.checkForDeadConnectionsLoopThread = threading.Thread(target=self.checkForDeadConnectionsLoop)
        self.checkForDeadConnectionsLoopThread.daemon = True
        self.checkForDeadConnectionsLoopThread.start()

    def getChannel(self):
        channel = InMemoryChannel(self)
        channel.start()

        guid = self.addConnection(channel)
        self.channels.append(channel)

        return channel, guid

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
