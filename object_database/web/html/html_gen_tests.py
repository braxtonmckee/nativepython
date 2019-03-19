from html_gen import *
from io import StringIO

import unittest

class HTMLGeneratorTests(unittest.TestCase):
    def test_add_child(self):
        test_child = HTMLElement()
        test_parent = HTMLElement()
        test_parent.add_child(test_child)
        self.assertTrue(test_child in test_parent.children)

    def test_add_child_self_closing(self):
        test_child = HTMLElement()
        test_parent = HTMLElement(is_self_closing=True)
        self.assertRaises(HTMLElementChildrenError, test_parent.add_child, test_child)

    def test_add_children(self):
        kids = [HTMLElement() for i in range(0, 10)]
        test_parent = HTMLElement()
        test_parent.add_children(kids)
        all_present = True
        for kid in kids:
            if kid not in test_parent.children:
                all_present = False
        self.assertTrue(all_present)

    def test_print_on(self):
        stream = StringIO()
        element = HTMLElement('div')
        element.attributes['class'] = 'column-4 medium'
        element.print_on(stream, newlines=False)
        output = stream.getvalue()
        self.assertEquals('<div class="column-4 medium"></div>', output)
