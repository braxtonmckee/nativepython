from abc import ABC, abstractmethod
from io import StringIO
from collections import defaultdict


class HTMLElementChildrenError(Exception):
    pass


class HTMLElementError(Exception):
    pass


class AbstractHTMLWriter(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def print_on(self, io_stream, indent=0, indent_increment=4, newlines=True):
        pass


class HTMLElement(AbstractHTMLWriter):
    def __init__(self, tag_name=None, attributes={}, children=[], is_self_closing=False):
        self.tag_name = tag_name
        self.is_self_closing = is_self_closing
        self.attributes = attributes
        self.children = children
        self.parent = None

    def add_child(self, child_element):
        if self.is_self_closing:
            raise HTMLElementChildrenError(
                '{} elements do not have children'.format(self.tag_name))
        elif child_element.parent:
            child_element.parent.remove_child(child_element)
        self.children.append(child_element)
        child_element.parent = self

    def remove_child(self, child_element):
        if child_element not in self.children:
            return
        self.children = [kid for kid in self.children if kid != child_element]

    def add_children(self, list_of_children):
        for child in list_of_children:
            self.add_child(child)

    def add_class(self, cls_string):
        if 'class' in self.attributes:
            current = self.attributes['class'].split()
            current_set = set(current)
            current_set.add(cls_string)
            self.attributes['class'] = " ".join(list(current_set))

    def remove_class(self, cls_string):
        if 'class' in self.attributes:
            current = self.attributes['class'].split()
            current_set = set(current)
            if cls_string not in current_set:
                return
            current_set.remove(cls_string)
            self.attributes['class'] = " ".join(list(current_set))

    def __str__(self):
        stream = StringIO()
        self.print_on(stream)
        return stream.getvalue()

    def __repr__(self):
        return "<{} [{}]>".format(self.__class__.__name__, self.tag_name)

    def pretty_print(self, indent_increment=2):
        stream = StringIO()
        self.print_on(stream, indent_increment=indent_increment)
        return stream.getvalue()

    def print_on(self, io_stream, indent=0, indent_increment=4, newlines=True):
        self._print_open_tag_on(io_stream, indent, newlines)
        if not self.is_self_closing:
            self._print_children_on(io_stream, indent, indent_increment)
            self._print_close_tag_on(io_stream, indent, newlines)

    def _print_open_tag_on(self, io_stream, indent, newlines):
        inline_indent = ' ' * indent
        io_stream.write('{}<{}'.format(inline_indent, self.tag_name))
        self._print_attributes_on(io_stream)
        if self.is_self_closing:
            io_stream.write('/>')
        else:
            io_stream.write('>')
        if newlines:
            io_stream.write('\n')

    def _print_close_tag_on(self, io_stream, indent, newlines):
        inline_indent = ' ' * indent
        io_stream.write('{}</{}>'.format(inline_indent, self.tag_name))
        if newlines:
            io_stream.write('\n')

    def _print_attributes_on(self, io_stream):
        for key, val in self.attributes.items():
            if len(val) > 0:
                io_stream.write(
                    ' {}="{}"'.format(key, val))

    def _print_children_on(self, io_stream, indent=0, indent_increment=4):
        if len(self.children) == 0:
            return
        indent += indent_increment
        for child in self.children:
            if isinstance(child, AbstractHTMLWriter):
                child.print_on(io_stream, indent, indent_increment)
            else:
                io_stream.write(child.__str__())

    @classmethod
    def div(cls, *args, **kwargs):
        return cls('div', *args, **kwargs)

    @classmethod
    def p(cls, *args, **kwargs):
        return cls('p', *args, **kwargs)

    @classmethod
    def img(cls, *args, **kwargs):
        return cls('img', is_self_closing=True, *args, **kwargs)


class HTMLTextContent(AbstractHTMLWriter):
    def __init__(self, content):
        super().__init__()
        self.content = content

    def print_on(self, io_stream, indent=0, indent_increment=4, newlines=True):
        for line in self.content.split("\n"):
            inline_indent = " " * indent
            io_stream.write('{}{}\n'.format(inline_indent, line))
