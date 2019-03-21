#!/usr/bin/env python3

import re
import unittest

from object_database.web.html.html_gen import (
    HTMLElement,
    HTMLTextContent,
    HTMLElementChildrenError
)

from cells import Card, Text


class CellHtmlTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_card(self):
        card = Card(Text("HI"))

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
