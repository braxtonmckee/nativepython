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

from object_database.core_schema import core_schema
from object_database.service_manager.ServiceSchema import service_schema
from object_database.service_manager.ServiceBase import ServiceBase, ServiceRuntimeConfig

import logging
import os
import threading
import time
import traceback


class ServiceWorker:
    def __init__(self, dbConnectionFactory, instance_id, storageRoot, serviceToken):
        self._logger = logging.getLogger(__name__)
        self.dbConnectionFactory = dbConnectionFactory
        self.db = dbConnectionFactory()
        self.db.subscribeToSchema(core_schema)

        # explicitly don't subscribe to everyone else's service hosts!
        self.db.subscribeToType(service_schema.Service)
        self.db.subscribeToType(service_schema.Codebase, lazySubscription=True)
        self.db.subscribeToType(service_schema.File, lazySubscription=True)

        self.runtimeConfig = ServiceRuntimeConfig(storageRoot, serviceToken)

        if not os.path.exists(storageRoot):
            os.makedirs(storageRoot)

        self.instance = service_schema.ServiceInstance.fromIdentity(instance_id)

        self.db.subscribeToObject(self.instance)

        with self.db.view():
            if self.instance.service.codebase is None:
                context = None
            else:
                context = self.instance.service.codebase.instantiate().serializationContext

        if context is not None:
            self.db.setSerializationContext(context)

        with self.db.view():
            host = self.instance.host
        self.db.subscribeToObject(host)

        self.serviceObject = None
        self.serviceName = None

        self.serviceWorkerThread = threading.Thread(target=self.synchronouslyRunService)
        self.serviceWorkerThread.daemon = True
        self.shouldStop = threading.Event()

        self.shutdownPollThread = threading.Thread(target=self.checkForShutdown)
        self.shutdownPollThread.daemon = True

    def initialize(self):
        assert self.db.waitForCondition(lambda: self.instance.exists(), 5.0)

        with self.db.transaction():
            assert self.instance.exists(), "Service Instance object %s doesn't exist" % self.instance._identity
            assert self.instance.service.exists(), "Service object %s doesn't exist" % self.instance.service._identity
            self.serviceName = self.instance.service.name
            self.instance.connection = self.db.connectionObject
            self.instance.codebase = self.instance.service.codebase
            self.instance.start_timestamp = time.time()
            self.instance.state = "Initializing"

            try:
                self.serviceObject = self._instantiateServiceObject()
            except Exception:
                self._logger.error('Service thread for %s failed:\n%s', self.instance._identity, traceback.format_exc())
                self.instance.markFailedToStart(traceback.format_exc())
                return
        try:
            self._logger.info("Initializing service object for %s", self.instance._identity)
            self.serviceObject.initialize()
        except Exception:
            self._logger.error('Service thread for %s failed:\n%s', self.instance._identity, traceback.format_exc())

            self.serviceObject = None

            with self.db.transaction():
                self.instance.markFailedToStart(traceback.format_exc())
                return

    def checkForShutdown(self):
        while not self.shouldStop.is_set():
            with self.db.view():
                if self.instance.shouldShutdown:
                    self.shouldStop.set()
                    return

            time.sleep(1.0)

    def synchronouslyRunService(self):
        self.initialize()

        if self.serviceObject is None:
            self.shouldStop.set()
            return

        with self.db.transaction():
            self.instance.state = "Running"

        try:
            self._logger.info("Starting runloop for service object %s", self.instance._identity)
            self.serviceObject.doWork(self.shouldStop)
        except Exception:
            self._logger.error(
                "Service %s/%s failed: %s",
                self.serviceName,
                self.instance._identity,
                traceback.format_exc()
            )

            with self.db.transaction():
                self.instance.state = "Crashed"
                self.instance.end_timestamp = time.time()
                self.instance.failureReason = traceback.format_exc()
                return
        else:
            with self.db.transaction():
                self._logger.info(
                    "Service %s/%s exited gracefully. Setting stopped flag.",
                    self.serviceName,
                    self.instance._identity
                )

                self.instance.state = "Stopped"
                self.instance.end_timestamp = time.time()

    def start(self):
        self.serviceWorkerThread.start()
        self.shutdownPollThread.start()

    def runAndWaitForShutdown(self):
        self.start()
        self.serviceWorkerThread.join()

    def stop(self):
        self.shouldStop.set()
        if self.serviceWorkerThread.isAlive():
            self.serviceWorkerThread.join()
        if self.shutdownPollThread.isAlive():
            self.shutdownPollThread.join()

    def _instantiateServiceObject(self):
        service_type = self.instance.service.instantiateServiceType()

        assert isinstance(service_type, type), service_type
        assert issubclass(service_type, ServiceBase), service_type

        service = service_type(self.db, self.instance.service, self.runtimeConfig)

        return service

    def isRunning(self):
        return self.serviceWorkerThread.isAlive()
