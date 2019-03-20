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

import unittest
import subprocess
import sys
import os
import time
from object_database.util import genToken


own_dir = os.path.split(__file__)[0]


class ObjectDatabaseFrontEnd(unittest.TestCase):
    def test_can_run_throughput_test(self):
        try:
            token = genToken()
            server = subprocess.Popen([
                sys.executable,
                os.path.join(own_dir, "frontends", "database_server.py"),
                "localhost", "8889",
                "--service-token", token,
                "--inmem"]
            )

            time.sleep(2.5)

            client = subprocess.run([
                sys.executable,
                os.path.join(own_dir, "frontends", "database_throughput_test.py"),
                "localhost", "8889",
                "--service-token", token,
                "1"
            ])

            self.assertEqual(client.returncode, 0)
        finally:
            server.terminate()
            server.wait()
