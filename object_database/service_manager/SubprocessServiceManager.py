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

import os
import shutil
import subprocess
import sys
import logging
import threading
import time
import traceback

from object_database.service_manager.ServiceManager import ServiceManager
from object_database.service_manager.ServiceSchema import service_schema
from object_database.util import validateLogLevel
from object_database import connect


ownDir = os.path.dirname(os.path.abspath(__file__))


def timestampToFileString(timestamp):
    struct = time.localtime(timestamp)
    return "%4d%02d%02d_%02d%02d%02d_%03d" % (
        struct.tm_year,
        struct.tm_mon,
        struct.tm_mday,
        struct.tm_hour,
        struct.tm_min,
        struct.tm_sec,
        int(timestamp*1000) % 1000
    )


def parseLogfileToInstanceid(fname):
    if not fname.endswith(".log.txt") or "-" not in fname:
        return
    return fname.split("-")[-1][:-8]


class SubprocessServiceManager(ServiceManager):
    def __init__(self, ownHostname, host, port,
                 sourceDir, storageDir, serviceToken,
                 isMaster, maxGbRam=4, maxCores=4, logfileDirectory=None,
                 shutdownTimeout=None, logLevelName="INFO"):
        self.host = host
        self.port = port
        self.storageDir = storageDir
        self.serviceToken = serviceToken
        self.logfileDirectory = logfileDirectory
        self.logLevelName = validateLogLevel(logLevelName, fallback='INFO')

        self.lock = threading.Lock()

        if logfileDirectory is not None:
            if not os.path.exists(logfileDirectory):
                os.makedirs(logfileDirectory)

        if not os.path.exists(storageDir):
            os.makedirs(storageDir)

        if not os.path.exists(sourceDir):
            os.makedirs(sourceDir)

        def dbConnectionFactory():
            return connect(host, port, self.serviceToken)

        ServiceManager.__init__(
            self, dbConnectionFactory, sourceDir, isMaster, ownHostname,
            maxGbRam=maxGbRam, maxCores=maxCores, shutdownTimeout=shutdownTimeout
        )
        self.serviceProcesses = {}
        self._logger = logging.getLogger(__name__)

    def startServiceWorker(self, service, instanceIdentity):
        with self.db.view():
            if instanceIdentity in self.serviceProcesses:
                return

            with self.lock:
                logfileName = (
                    service.name + "-" + timestampToFileString(time.time())
                    + "-" + str(instanceIdentity) + ".log.txt"
                )

                if self.logfileDirectory is not None:
                    output_file = open(os.path.join(self.logfileDirectory, logfileName), "w")
                else:
                    output_file = None

                process = subprocess.Popen(
                    [
                        sys.executable,
                        os.path.join(ownDir, '..', 'frontends', 'service_entrypoint.py'),
                        service.name,
                        self.host,
                        str(self.port),
                        str(instanceIdentity),
                        os.path.join(self.sourceDir, str(instanceIdentity)),
                        os.path.join(self.storageDir, str(instanceIdentity)),
                        self.serviceToken,
                        '--log-level', self.logLevelName
                    ],
                    cwd=self.storageDir,
                    stdin=subprocess.DEVNULL,
                    stdout=output_file,
                    stderr=subprocess.STDOUT
                )

                self.serviceProcesses[instanceIdentity] = process

                if output_file:
                    output_file.close()

            if self.logfileDirectory:
                self._logger.info(
                    "Started a service logging to %s with pid %s",
                    os.path.join(self.logfileDirectory, logfileName),
                    process.pid
                )
            else:
                self._logger.info(
                    "Started service %s/%s with pid %s",
                    service.name,
                    instanceIdentity,
                    process.pid
                )

    def stop(self, gracefully=True):
        if gracefully:
            self.stopAllServices(self.shutdownTimeout)

        ServiceManager.stop(self)

        with self.lock:
            for instanceIdentity, workerProcess in self.serviceProcesses.items():
                workerProcess.terminate()

            for instanceIdentity, workerProcess in self.serviceProcesses.items():
                workerProcess.wait()

        self.serviceProcesses = {}

    def cleanup(self):
        for identity, workerProcess in list(self.serviceProcesses.items()):
            if workerProcess.poll() is not None:
                workerProcess.wait()
                del self.serviceProcesses[identity]

        with self.db.view():
            for identity in list(self.serviceProcesses):
                serviceInstance = service_schema.ServiceInstance.fromIdentity(identity)

                if (not serviceInstance.exists()
                        or (serviceInstance.shouldShutdown and
                            time.time() - serviceInstance.shutdownTimestamp > self.shutdownTimeout)):
                    workerProcess = self.serviceProcesses.get(identity)
                    if workerProcess:
                        workerProcess.terminate()
                        workerProcess.wait()
                        del self.serviceProcesses[identity]

        self.cleanupOldLogfiles()

    def extractLogData(self, targetInstanceId, maxBytes):
        if self.logfileDirectory:
            with self.lock:
                for file in os.listdir(self.logfileDirectory):
                    instanceId = parseLogfileToInstanceid(file)
                    if instanceId and instanceId == targetInstanceId:
                        fpath = os.path.join(self.logfileDirectory, file)
                        with open(fpath, "r") as f:
                            f.seek(0, 2)
                            curPos = f.tell()
                            f.seek(max(curPos-maxBytes, 0))

                            return f.read()

        return "<logfile not found>"

    def cleanupOldLogfiles(self):
        if self.logfileDirectory:
            with self.lock:
                for file in os.listdir(self.logfileDirectory):
                    instanceId = parseLogfileToInstanceid(file)
                    if instanceId and instanceId not in self.serviceProcesses:
                        if not os.path.exists(os.path.join(self.logfileDirectory, "old")):
                            os.makedirs(os.path.join(self.logfileDirectory, "old"))
                        shutil.move(
                            os.path.join(self.logfileDirectory, file),
                            os.path.join(self.logfileDirectory, "old", file)
                        )

        if self.storageDir:
            with self.lock:
                if os.path.exists(self.storageDir):
                    for file in os.listdir(self.storageDir):
                        if file not in self.serviceProcesses:
                            try:
                                path = os.path.join(self.storageDir, file)
                                self._logger.info("Removing storage at path %s for dead service.", path)
                                shutil.rmtree(path)
                            except Exception:
                                self._logger.error(
                                    "Failed to remove storage at path %s for dead service:\n%s",
                                    path,
                                    traceback.format_exc()
                                )

        if self.sourceDir:
            with self.lock:
                if os.path.exists(self.sourceDir):
                    for file in os.listdir(self.sourceDir):
                        if file not in self.serviceProcesses:
                            try:
                                path = os.path.join(self.sourceDir, file)
                                self._logger.info("Removing source caches at path %s for dead service.", path)
                                shutil.rmtree(path)
                            except Exception:
                                self._logger.error(
                                    "Failed to remove source cache at path %s for dead service:\n%s",
                                    path,
                                    traceback.format_exc()
                                )
