#!/usr/bin/env python3

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

import argparse
import logging
import sys
import traceback

from object_database import connect
from object_database.util import validateLogLevel, configureLogging
from object_database.service_manager.Codebase import setCodebaseInstantiationDirectory
from object_database.service_manager.ServiceWorker import ServiceWorker


def main(argv):
    parser = argparse.ArgumentParser("Run a specific service.")

    parser.add_argument("servicename")
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("instanceid", type=int)
    parser.add_argument("sourceDir")
    parser.add_argument("storageRoot")
    parser.add_argument("serviceToken")
    parser.add_argument("--log-level", required=False, default="INFO")

    parsedArgs = parser.parse_args(argv[1:])

    level = parsedArgs.log_level.upper()
    level = validateLogLevel(level, fallback='INFO')

    configureLogging(preamble=str(parsedArgs.instanceid), level=level)

    logger = logging.getLogger(__name__)

    logger.info(
        "service_entrypoint.py connecting to %s:%s for %s",
        parsedArgs.host,
        parsedArgs.port,
        parsedArgs.instanceid
    )

    def dbConnectionFactory():
        return connect(parsedArgs.host, parsedArgs.port, parsedArgs.serviceToken)

    setCodebaseInstantiationDirectory(parsedArgs.sourceDir)

    try:
        manager = ServiceWorker(
            dbConnectionFactory,
            int(parsedArgs.instanceid),
            parsedArgs.storageRoot,
            parsedArgs.serviceToken
        )

        manager.runAndWaitForShutdown()

        return 0
    except Exception:
        logger.error("service_entrypoint failed with an exception:\n%s", traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
