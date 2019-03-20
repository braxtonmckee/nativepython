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

# flake8: noqa

# ensure typed_python is imported because our extension needs it
import typed_python._types
import ctypes

# make sure that 'typed_python' is loaded with global symbol resolution
ctypes.CDLL(typed_python._types.__file__, mode=ctypes.RTLD_GLOBAL)

import object_database._types

from object_database.tcp_server import connect, TcpServer
from object_database.persistence import RedisPersistence, InMemoryPersistence
from object_database.schema import Schema, Indexed, Index, SubscribeLazilyByDefault
from object_database.core_schema import core_schema
from object_database.object import DatabaseObject
from object_database.service_manager.ServiceSchema import service_schema
from object_database.service_manager.Codebase import Codebase
from object_database.service_manager.ServiceBase import ServiceBase
from object_database.view import revisionConflictRetry, RevisionConflictException, DisconnectedException, current_transaction
from object_database.inmem_server import InMemServer
