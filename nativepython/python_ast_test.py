#   Copyright 2017 Braxton Mckee
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

import nativepython.python_ast as python_ast
import unittest
import ast

class PythonASTTests(unittest.TestCase):
    def test_conversion(self):
        conversion = python_ast.convertPyAstToAlgebraic(ast.parse("lambda x: x+y+1.0", mode='eval'),"<eval>")
        self.assertTrue(conversion.matches.Expression)
        self.assertTrue(conversion.body.matches.Lambda)
        self.assertTrue(conversion.body.args.matches.Item)
        self.assertTrue(len(conversion.body.args.args) == 1)

