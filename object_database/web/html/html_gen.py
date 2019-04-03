"""HTML String Generation

This module contains classes for creating bare-bones
HTML string generation. The included classes cover all
text-node and HTML5 tag cases
"""
import inspect

from abc import ABC, abstractmethod
from functools import partial
from io import StringIO


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
    """HTML element generation class.

    This class can generate correctly-formatted HTML
    corresponding to any valid HTML5 element.
    It contains a collection of children who it also
    knows how to format.

    Notes
    _____
    HTMLElement and children use the `print_on` method to
    write the string representations of their HTML to
    a stream (StringIO stream in this implementation).
    A Given HTMLElement's children are written to this
    stream recursively before any close tags (see docstring
    for `print_on`)

    We also use some meta properties to add convenience
    constructors to this class that allow for easier to
    read composition by end users. See the Examples.

    Example
    -------
    Any valid HTML5 tag can be created using a convenience
    constructor that is simply its name. These handle
    knowing and configuring whether or not an element is
    self closing::
        element = HTMLElement.div().with_children(
            HTMLElement.img())
        print(element)
        # <div>
        #     <img/>
        # </div>

    You can also construct elements manually by specifying
    the tagname as the first argument::
        element = HTMLElement('div').with_children(
            HTMLElement('img', is_self_closing=True))

    All methods besides private and basic printing
    methods return `self` and are therefore chainable::
        element = HTMLElement.div() \
                             .add_class('container') \
                             .with_children(
                                 HTMLElement.img(),
                                 HTMLElement.br())
    """

    def __init__(self, tag_name=None, attributes=None, children=None,
                 is_self_closing=False):
        """
        Parameters:
        ----------
        tag_name: str
            example: div, p, a etc
        attributes : dict
            html tag attributes
        children : list
            list of HTMLElement or HTMLTextContent
        is_self_closing : bool
            a self-closing tag does not carry a content payload;
            example: <img src="path to img"/>

        """
        self.tag_name = tag_name
        self.is_self_closing = is_self_closing
        self.attributes = attributes if attributes is not None else {}
        self.children = children
        self.parent = None

    def add_child(self, child_element):
        """Adds a child element

        To prevent circular references, this method will
        remove the passed `child_element` from its parent
        (if present) and will set the new parent to the
        current instance.

        Parameters
        ----------
        child_element: AbstractHTMLWriter
            Any subclass of AbstractHTMLWriter that implements
            a `print_on` method.

        Returns
        -------
        HTMLElement
            A reference to the current element instance
            that can be used for chaining

        Raises
        ------
        HTMLElementChildrenError
            If the current instance is self closing
            (and therefore doesn't take children)
        """
        if self.is_self_closing:
            raise HTMLElementChildrenError(
                '{} elements do not have children'.format(self.tag_name))
        elif child_element.parent:
            child_element.parent.remove_child(child_element)
        if self.children is None:
            self.children = [child_element]
        else:
            self.children.append(child_element)
        child_element.parent = self
        return self

    def remove_child(self, child_element):
        """Removes a child element

        Parameters
        ----------
        child_element: AbstractHTMLWriter
            Any subclass of AbstractHTMLWriter that implements
            a `print_on` method.

        Returns
        -------
        HTMLElement
            A reference to the current element instance
            that can be used for chaining.

        Raises
        ------
        HTMLElementChildrenError
            If the current instance is self closing
            (and therefore doesn't have children)
        """
        if self.children is None or child_element not in self.children:
            raise HTMLElementChildrenError(
                '{} elements does not have children'.format(self.tag_name))
        self.children = [kid for kid in self.children if kid != child_element]
        return self

    def add_children(self, list_of_children):
        """Adds all children from a provided list of child elements

        Parameters
        ----------
        list_of_children: list
            A list of any AbstractHTMLWriter subclass instances.

        Returns
        -------
        HTMLElement
            A reference to the current element instance
            that can be used for chaining.
        """
        for child in list_of_children:
            self.add_child(child)
        return self

    def with_children(self, *args):
        """Adds children to current element using positional args

        This is a convenience function that can be used
        for a different type of composition than `add_children`.
        Instead of taking a list, it takes an unspecified number
        of arguments, each of which is an element of some sort.

        Parameters
        ----------
        *args
            A variable length argument list where each argument
            is an instance of any AbstractHTMLWriter subclass.

        Returns
        -------
        HTMLElement
            A reference to the current element instance
            that can be used for chaining.
        """
        self.add_children(args)
        return self

    def add_class(self, cls_string):
        """Adds an HTML element class value to current class attributes

        Example
        -------
        Adding a class will modify the `attributes['class'] string
        value::
            initial_attrs = {'class': 'primary column-4'}
            element = HTMLElement('div', attributes=initial_attrs)
            element.add_class('medium')
            print(element.attributes['class'])
            # 'primary column-4 medium'

        Parameters
        ----------
        cls_string: str
            A string of an html class attribute to add to
            current classes for the element

        Returns
        -------
        HTMLElement
            A reference to the current element instance
            that can be used for chaining.
        """
        if 'class' in self.attributes:
            class_list = self.attributes['class'].split()
            if cls_string not in class_list:
                class_list.append(cls_string)
                self.attributes['class'] = " ".join(class_list)
        else:
            self.attributes["class"] = cls_string
        return self

    def remove_class(self, cls_string):
        """Removes an HTML element class value from the current attributes

        Example
        -------
        Removing a class will modify the `attributes['class'] string
        value::
            initial_attrs = {'class': 'primary column-4 medium'}
            element = HTMLElement('div', attributes=initial_attrs)
            element.remove_class('medium')
            print(element.attributes['class'])
            # 'primary column-4'

        Parameters
        ----------
        cls_string: str
            A string of an html class attribute to remove
            from the current element's class attribute.

        Returns
        -------
        HTMLElement
            A reference to the current element instance
            that can be used for chaining.
        """
        if 'class' in self.attributes:
            class_list = self.attributes["class"].split()
            if cls_string in class_list:
                class_list.remove(cls_string)
            self.attributes['class'] = " ".join(class_list)
        return self

    def add_classes(self, list_of_str):
        """Adds HTML element class values from a list

        Example
        -------
        Add three classes from a list::
            classes_to_add = ["one", "two", "three"]
            element = HTMLElement()
            element.add_classes(classes_to_add)
            print(element.attributes["class"])
            # 'one two three'

        Parameters
        ----------
        list_of_str: list
            A list of strings that are HTML class values

        Returns
        -------
        HTMLElement
            A reference to the current instance that can be
            used for chaining calls.
        """
        for class_str in list_of_str:
            self.add_class(class_str)
        return self

    def set_attribute(self, attribute_name, attribute_val):
        """Convenience method for settings specific attributes.

        This method should be used for setting arbitrary
        HTML attributes and values. It is included for
        convenience purposes and designed to be used
        when chaining method calls.

        Example
        -------
        Set both a `tabindex` and `role` attribute
        on a `div` element using chaining::
            element = (
                HTMLElement.div()
                           .set_attribute('role', 'primary')
                           .set_attribute('tabindex', '2')
                           .add_class('default-window')
                )

        Parameters
        ----------
        attribute_name: str
            The name (key)of the HTML attribute to set
        attribute_val: str
            The value to set for the given attribute

        Returns
        -------
        HTMLElement
            A reference to the current instance that
            can be used for chaining method calls.
        """
        self.attributes[attribute_name] = attribute_val
        return self

    def pretty_print(self, indent_increment=2):
        """Prints the HTML structure of the element with custom indentation

        Parameters
        ----------
        indent_increment: int
            The number of spaces to indent each new
            level of the hierarchy as nested elements
            are printed to the string.

        Returns
        -------
        str
            A printed string of the HTML structure using
            the specified indentation
        """
        stream = StringIO()
        self.print_on(stream, indent_increment=indent_increment)
        return stream.getvalue()

    def print_on(self, io_stream, indent=0, indent_increment=4, newlines=True):
        """Prints the entire HTML representation to a stream.

        Notes
        -----
        This is a required method of all AbstractHTMLWriter subclasses.
        In this implementation, an element prints its opening tag and
        if it takes and has children, recursively calls each child's
        corresponding `print_on` using the same stream. In the end
        it prints its closing tag.

        This implementation makes use of default arguments for
        newlines and indentation which can be customized
        (see `pretty_print` for an example)

        Parameters:
        ----------
        io_stream: StringIO
            A stream that we can write strings to.
            Can be any stream object that implements
            a `write` method.
        indent: int
            The current indentation level. Each recursively
            rendered child will increment this amount by
            the passed `indent_increment`.
        indent_increment: int
            The indentation level to use (number of spaces)
        newlines: bool
            Whether or not to print using newlines
        """
        self._print_open_tag_on(io_stream, indent, newlines)
        if not self.is_self_closing:
            self._print_children_on(io_stream, indent, indent_increment)
            self._print_close_tag_on(io_stream, indent, newlines)

    def _print_open_tag_on(self, io_stream, indent, newlines):
        """Prints the correct opening HTML tag for the element to stream."""
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
        """Prints the correct closing for the element to the stream"""
        inline_indent = ' ' * indent
        io_stream.write('{}</{}>'.format(inline_indent, self.tag_name))
        if newlines:
            io_stream.write('\n')

    def _print_attributes_on(self, io_stream):
        """Prints the attributes of the element to the stream"""
        for key, val in self.attributes.items():
            if len(val) > 0:
                io_stream.write(
                    ' {}="{}"'.format(key, val))

    def _print_children_on(self, io_stream, indent=0, indent_increment=4):
        """Recursively prints the HTML representation of
        a child element to the stream.
        """
        if self.children is None or len(self.children) == 0:
            return
        indent += indent_increment
        for child in self.children:
            if isinstance(child, AbstractHTMLWriter):
                child.print_on(io_stream, indent, indent_increment)
            else:
                io_stream.write(child.__str__())

    def __str__(self):
        stream = StringIO()
        self.print_on(stream)
        return stream.getvalue()

    def __repr__(self):
        return "<{} [{}]>".format(self.__class__.__name__, self.tag_name)


# HELPERS
# For the sake of brevity we set HTMLElement bound class methods, i.e. html
# "tags" dynamically. TODO: HTML_TAG_CONFIG should be moved to another file?

def _func(cls, *args, **kwargs):
    """Helper function for setting classmethods."""
    return cls(*args, **kwargs)


HTML_TAG_CONFIG = [
    {"tag_name": "a"},
    {"tag_name": "abbr"},
    {"tag_name": "address"},
    {"is_self_closing": True, "tag_name": "area"},
    {"tag_name": "article"},
    {"tag_name": "aside"},
    {"tag_name": "audio"},
    {"tag_name": "b"},
    {"is_self_closing": True, "tag_name": "base"},
    {"tag_name": "bdi"},
    {"tag_name": "bdo"},
    {"tag_name": "blockquote"},
    {"tag_name": "body"},
    {"is_self_closing": True, "tag_name": "br"},
    {"tag_name": "button"},
    {"tag_name": "canvas"},
    {"tag_name": "caption"},
    {"tag_name": "cite"},
    {"tag_name": "code"},
    {"is_self_closing": True, "tag_name": "col"},
    {"tag_name": "colgroup"},
    {"tag_name": "data"},
    {"tag_name": "datalist"},
    {"tag_name": "dd"},
    {"tag_name": "_del"},  # decided not to overload the "del" destructor
    {"tag_name": "details"},
    {"tag_name": "dfn"},
    {"tag_name": "dialog"},
    {"tag_name": "div"},
    {"tag_name": "dl"},
    {"tag_name": "dt"},
    {"tag_name": "em"},
    {"is_self_closing": True, "tag_name": "embed"},
    {"tag_name": "fieldset"},
    {"tag_name": "figcaption"},
    {"tag_name": "figure"},
    {"tag_name": "footer"},
    {"tag_name": "form"},
    {"tag_name": "h1"},
    {"tag_name": "h2"},
    {"tag_name": "h3"},
    {"tag_name": "h4"},
    {"tag_name": "h5"},
    {"tag_name": "h6"},
    {"tag_name": "head"},
    {"tag_name": "header"},
    {"tag_name": "hgroup"},
    {"is_self_closing": True, "tag_name": "hr"},
    {"tag_name": "html"},
    {"tag_name": "i"},
    {"tag_name": "iframe"},
    {"is_self_closing": True, "tag_name": "img"},
    {"is_self_closing": True, "tag_name": "input"},
    {"tag_name": "ins"},
    {"tag_name": "kbd"},
    {"tag_name": "keygen"},
    {"tag_name": "label"},
    {"tag_name": "legend"},
    {"tag_name": "li"},
    {"is_self_closing": True, "tag_name": "link"},
    {"tag_name": "main"},
    {"tag_name": "map"},
    {"tag_name": "mark"},
    {"tag_name": "math"},
    {"tag_name": "menu"},
    {"tag_name": "menuitem"},
    {"is_self_closing": True, "tag_name": "meta"},
    {"tag_name": "meter"},
    {"tag_name": "nav"},
    {"tag_name": "noscript"},
    {"tag_name": "object"},
    {"tag_name": "ol"},
    {"tag_name": "optgroup"},
    {"tag_name": "option"},
    {"tag_name": "output"},
    {"tag_name": "p"},
    {"is_self_closing": True, "tag_name": "param"},
    {"tag_name": "picture"},
    {"tag_name": "pre"},
    {"tag_name": "progress"},
    {"tag_name": "q"},
    {"tag_name": "rb"},
    {"tag_name": "rp"},
    {"tag_name": "rt"},
    {"tag_name": "rtc"},
    {"tag_name": "ruby"},
    {"tag_name": "s"},
    {"tag_name": "samp"},
    {"tag_name": "script"},
    {"tag_name": "section"},
    {"tag_name": "select"},
    {"tag_name": "slot"},
    {"tag_name": "small"},
    {"is_self_closing": True, "tag_name": "source"},
    {"tag_name": "span"},
    {"tag_name": "strong"},
    {"tag_name": "style"},
    {"tag_name": "sub"},
    {"tag_name": "summary"},
    {"tag_name": "sup"},
    {"tag_name": "svg"},
    {"tag_name": "table"},
    {"tag_name": "tbody"},
    {"tag_name": "td"},
    {"tag_name": "template"},
    {"tag_name": "textarea"},
    {"tag_name": "tfoot"},
    {"tag_name": "th"},
    {"tag_name": "thead"},
    {"tag_name": "time"},
    {"tag_name": "title"},
    {"tag_name": "tr"},
    {"is_self_closing": True, "tag_name": "track"},
    {"tag_name": "u"},
    {"tag_name": "ul"},
    {"tag_name": "var"},
    {"tag_name": "video"},
    {"is_self_closing": True, "tag_name": "wbr"}]

for tag in HTML_TAG_CONFIG:
    setattr(HTMLElement, tag["tag_name"],
            classmethod(partial(_func, **tag)))


def _get_method_names(cls):
    """Helper static method for inspecting all avail. tags"""
    return [item[0] for item in
            inspect.getmembers(cls, predicate=inspect.ismethod)]


setattr(HTMLElement, "all_methods",
        staticmethod(_get_method_names(HTMLElement)))


class HTMLTextContent(AbstractHTMLWriter):
    """Raw text content adjacent to elements.

    This class is equivalent to a DOM text node
    that displays as a direct child of a given
    element.

    Example
    -------
    Here is how we could add text to a paragraph::
        paragraph = HTMLElement('p').with_children(
            HTMLTextContent('Here is the para text!'))
    """

    def __init__(self, content):
        """
        Parameters:
        ----------
        content: str
            Content that will be displayed
        """
        super().__init__()
        self.content = content
        self.parent = None

    def print_on(self, io_stream, indent=0, indent_increment=4, newlines=True):
        """Print the text content to a given stream

        Parameters:
        ----------
        io_stream: StringIO
            Any stream object that we can write strings
            to.
        indent: int
            The current indentation level
        indent_increment: int
            The amount to increase indendation by
        newlines: bool
            Whether or not to print with newlines
        """
        for line in self.content.splitlines():
            inline_indent = " " * indent
            if newlines:
                io_stream.write('{}{}\n'.format(inline_indent, line))
            else:
                io_stream.write('{}{} '.format(inline_indent, line))
