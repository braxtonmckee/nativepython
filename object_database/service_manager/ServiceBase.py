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


import object_database


class ServiceRuntimeConfig:
    def __init__(self, serviceTemporaryStorageRoot, serviceToken):
        self.serviceTemporaryStorageRoot = serviceTemporaryStorageRoot
        self.serviceToken = serviceToken


class ServiceBase:
    coresUsed = 1
    gbRamUsed = 1

    def __init__(self, db, serviceObject, runtimeConfig):
        self.db = db
        self.serviceObject = serviceObject
        self.runtimeConfig = runtimeConfig

        if self.serviceObject is not None:
            self.serializationContext = self.serviceObject.getSerializationContext()
        else:
            self.serializationContext = None

    @staticmethod
    def configureFromCommandline(db, serviceObject, args):
        """Subclasses should take the remaining args from the commandline and configure using them"""
        pass

    def initialize(self):
        pass

    def doWork(self, shouldStop):
        # subclasses actually do work in here.
        shouldStop.wait()

    @staticmethod
    def serviceDisplay(serviceObject, instance=None, objType=None, queryArgs=None):
        return object_database.web.cells.Card("No details provided for service '%s'" % serviceObject.name)

    @staticmethod
    def serviceHeaderToggles(serviceObject, instance=None):
        return []
