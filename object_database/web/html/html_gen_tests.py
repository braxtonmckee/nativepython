#!/usr/bin/env python3

import re
import unittest

from object_database.web.html.html_gen import (
    HTMLElement,
    HTMLTextContent,
    HTML_TAG_CONFIG,
    HTMLElementChildrenError
)
from io import StringIO


class HTMLGeneratorTests(unittest.TestCase):
    def setUp(self):
        self.current_tag_names = [item["tag_name"] for item in HTML_TAG_CONFIG]

    def test_add_child(self):
        test_child = HTMLElement()
        test_parent = HTMLElement()
        test_parent.add_child(test_child)
        self.assertTrue(test_child in test_parent.children)

    def test_add_child_self_closing(self):
        test_child = HTMLElement()
        test_parent = HTMLElement(is_self_closing=True)

        self.assertRaises(HTMLElementChildrenError, test_parent.add_child,
                          test_child)

    def test_add_children(self):
        kids = [HTMLElement() for i in range(0, 10)]
        test_parent = HTMLElement()
        test_parent.add_children(kids)
        all_present = True
        for kid in kids:
            if kid not in test_parent.children:
                all_present = False
        self.assertTrue(all_present)

    def test_add_class_on_empty(self):
        element = HTMLElement()
        element.add_class("column-4")
        class_list = element.attributes["class"].split()
        self.assertTrue("column-4" in class_list)

    def test_add_class(self):
        element = HTMLElement(attributes={'class': 'one two'})
        element.add_class("three")
        self.assertEqual(element.attributes['class'], "one two three")

    def test_remove_class(self):
        element = HTMLElement(attributes={"class": "one two three"})
        element.remove_class("two")
        self.assertEqual(element.attributes['class'], "one three")

    def test_remove_class_on_empty(self):
        element = HTMLElement()
        element.remove_class("test")
        self.assertTrue("class" not in element.attributes)

    def test_set_attribute(self):
        element = HTMLElement()
        element.set_attribute('role', 'primary')
        element.set_attribute('class', 'window column-4 centered')
        self.assertEqual(element.attributes['role'], 'primary')
        self.assertEqual(element.attributes['class'], 'window column-4 centered')

    def test_print_on_basic(self):
        stream = StringIO()
        element = HTMLElement('div')
        element.attributes['class'] = 'column-4 medium'
        element.print_on(stream, newlines=False)
        output = stream.getvalue()
        self.assertEqual('<div class="column-4 medium"></div>', output)

    def test_print_on_nested(self):
        stream = StringIO()
        child = HTMLElement('p')
        child.attributes['class'] = 'column-4 medium'
        parent = HTMLElement('div', children=[child])
        parent.attributes['class'] = 'column-4 medium'
        parent.print_on(stream, newlines=False)
        output = stream.getvalue()
        test_out = ('<div class="column-4 medium"><p class="column-4 medium">' +
                    '</p></div>')
        # we don't care about white spaces or new linesso much
        output = re.sub(r'\s{2,}', '', output)
        output = re.sub(r'\n', '', output)
        self.assertEqual(test_out, output)

    def test_print_on_with_content(self):
        stream = StringIO()
        element = HTMLTextContent('this is content')
        element.print_on(stream)
        output = stream.getvalue()
        test_out = "this is content\n"
        self.assertEqual(test_out, output)

    def test_print_on_with_content_nested(self):
        stream = StringIO()
        content = HTMLTextContent('this is content')
        parent = HTMLElement('div', children=[content])
        parent.attributes['class'] = 'column-4 medium'
        parent.print_on(stream, newlines=False)
        output = stream.getvalue()
        test_out = ('<div class="column-4 medium">this is content' +
                    '</div>')
        # we don't care about white spaces or new linesso much
        output = re.sub(r'\s{2,}', '', output)
        output = re.sub(r'\n', '', output)
        self.assertEqual(test_out, output)

    def test_print_on_set_boundmethod(self):
        stream = StringIO()
        element = HTMLElement.div()
        element.attributes['class'] = 'column-4 medium'
        element.print_on(stream, newlines=False)
        output = stream.getvalue()
        self.assertEqual('<div class="column-4 medium"></div>', output)

    def test_with_children(self):
        element = HTMLElement('div')
        child_one = HTMLElement('img')
        child_two = HTMLElement('article')
        element.with_children(child_one, child_two)
        self.assertTrue(child_one in element.children)
        self.assertTrue(child_two in element.children)

    def test_list_methods(self):
        method_names = HTMLElement.all_methods
        for name in self.current_tag_names:
            self.assertIn(name, method_names)

    def tearDown(self):
        pass


class HTMLChainingMethodTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_chaining_add_child(self):
        element = HTMLElement('div')
        result = element.add_child(HTMLElement())
        self.assertEqual(element, result)

    def test_chaining_remove_child(self):
        element = HTMLElement()
        child = HTMLElement()
        element.add_child(child)
        result = element.remove_child(child)
        self.assertEqual(element, result)

    def test_chaining_add_children(self):
        element = HTMLElement('div')
        children = [HTMLElement(), HTMLElement()]
        result = element.add_children(children)
        self.assertEqual(element, result)

    def test_chaining_with_children(self):
        element = HTMLElement('div')
        child_one = HTMLElement()
        child_two = HTMLElement()
        result = element.with_children(child_one, child_two)
        self.assertEqual(result, element)

    def test_chaining_set_attribute(self):
        element = HTMLElement()
        result = element.set_attribute('role', 'primary')
        self.assertEqual(element, result)

    def tearDown(self):
        pass


class HTMLCustomConstructorTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_a_constructor(self):
        element = HTMLElement.a()
        self.assertEqual(element.tag_name, 'a')
        self.assertFalse(element.is_self_closing)

    def test_abbr_constructor(self):
        element = HTMLElement.abbr()
        self.assertEqual(element.tag_name, 'abbr')
        self.assertFalse(element.is_self_closing)

    def test_address_constructor(self):
        element = HTMLElement.address()
        self.assertEqual(element.tag_name, 'address')
        self.assertFalse(element.is_self_closing)

    def test_area_constructor(self):
        element = HTMLElement.area()
        self.assertEqual(element.tag_name, 'area')
        self.assertTrue(element.is_self_closing)

    def test_article_constructor(self):
        element = HTMLElement.article()
        self.assertEqual(element.tag_name, 'article')
        self.assertFalse(element.is_self_closing)

    def test_aside_constructor(self):
        element = HTMLElement.aside()
        self.assertEqual(element.tag_name, 'aside')
        self.assertFalse(element.is_self_closing)

    def test_audio_constructor(self):
        element = HTMLElement.audio()
        self.assertEqual(element.tag_name, 'audio')
        self.assertFalse(element.is_self_closing)

    def test_b_constructor(self):
        element = HTMLElement.b()
        self.assertEqual(element.tag_name, 'b')
        self.assertFalse(element.is_self_closing)

    def test_base_constructor(self):
        element = HTMLElement.base()
        self.assertEqual(element.tag_name, 'base')
        self.assertTrue(element.is_self_closing)

    def test_bdi_constructor(self):
        element = HTMLElement.bdi()
        self.assertEqual(element.tag_name, 'bdi')
        self.assertFalse(element.is_self_closing)

    def test_bdo_constructor(self):
        element = HTMLElement.bdo()
        self.assertEqual(element.tag_name, 'bdo')
        self.assertFalse(element.is_self_closing)

    def test_blockquote_constructor(self):
        element = HTMLElement.blockquote()
        self.assertEqual(element.tag_name, 'blockquote')
        self.assertFalse(element.is_self_closing)

    def test_body_constructor(self):
        element = HTMLElement.body()
        self.assertEqual(element.tag_name, 'body')
        self.assertFalse(element.is_self_closing)

    def test_br_constructor(self):
        element = HTMLElement.br()
        self.assertEqual(element.tag_name, 'br')
        self.assertTrue(element.is_self_closing)

    def test_button_constructor(self):
        element = HTMLElement.button()
        self.assertEqual(element.tag_name, 'button')
        self.assertFalse(element.is_self_closing)

    def test_canvas_constructor(self):
        element = HTMLElement.canvas()
        self.assertEqual(element.tag_name, 'canvas')
        self.assertFalse(element.is_self_closing)

    def test_caption_constructor(self):
        element = HTMLElement.caption()
        self.assertEqual(element.tag_name, 'caption')
        self.assertFalse(element.is_self_closing)

    def test_cite_constructor(self):
        element = HTMLElement.cite()
        self.assertEqual(element.tag_name, 'cite')
        self.assertFalse(element.is_self_closing)

    def test_code_constructor(self):
        element = HTMLElement.code()
        self.assertEqual(element.tag_name, 'code')
        self.assertFalse(element.is_self_closing)

    def test_col_constructor(self):
        element = HTMLElement.col()
        self.assertEqual(element.tag_name, 'col')
        self.assertTrue(element.is_self_closing)

    def test_colgroup_constructor(self):
        element = HTMLElement.colgroup()
        self.assertEqual(element.tag_name, 'colgroup')
        self.assertFalse(element.is_self_closing)

    def test_data_constructor(self):
        element = HTMLElement.data()
        self.assertEqual(element.tag_name, 'data')
        self.assertFalse(element.is_self_closing)

    def test_datalist_constructor(self):
        element = HTMLElement.datalist()
        self.assertEqual(element.tag_name, 'datalist')
        self.assertFalse(element.is_self_closing)

    def test_dd_constructor(self):
        element = HTMLElement.dd()
        self.assertEqual(element.tag_name, 'dd')
        self.assertFalse(element.is_self_closing)

    def test_del_constructor(self):
        element = HTMLElement._del()
        self.assertEqual(element.tag_name, '_del')
        self.assertFalse(element.is_self_closing)

    def test_details_constructor(self):
        element = HTMLElement.details()
        self.assertEqual(element.tag_name, 'details')
        self.assertFalse(element.is_self_closing)

    def test_dfn_constructor(self):
        element = HTMLElement.dfn()
        self.assertEqual(element.tag_name, 'dfn')
        self.assertFalse(element.is_self_closing)

    def test_dialog_constructor(self):
        element = HTMLElement.dialog()
        self.assertEqual(element.tag_name, 'dialog')
        self.assertFalse(element.is_self_closing)

    def test_div_constructor(self):
        element = HTMLElement.div()
        self.assertEqual(element.tag_name, 'div')
        self.assertFalse(element.is_self_closing)

    def test_dl_constructor(self):
        element = HTMLElement.dl()
        self.assertEqual(element.tag_name, 'dl')
        self.assertFalse(element.is_self_closing)

    def test_dt_constructor(self):
        element = HTMLElement.dt()
        self.assertEqual(element.tag_name, 'dt')
        self.assertFalse(element.is_self_closing)

    def test_em_constructor(self):
        element = HTMLElement.em()
        self.assertEqual(element.tag_name, 'em')
        self.assertFalse(element.is_self_closing)

    def test_embed_constructor(self):
        element = HTMLElement.embed()
        self.assertEqual(element.tag_name, 'embed')
        self.assertTrue(element.is_self_closing)

    def test_fieldset_constructor(self):
        element = HTMLElement.fieldset()
        self.assertEqual(element.tag_name, 'fieldset')
        self.assertFalse(element.is_self_closing)

    def test_figcaption_constructor(self):
        element = HTMLElement.figcaption()
        self.assertEqual(element.tag_name, 'figcaption')
        self.assertFalse(element.is_self_closing)

    def test_figure_constructor(self):
        element = HTMLElement.figure()
        self.assertEqual(element.tag_name, 'figure')
        self.assertFalse(element.is_self_closing)

    def test_footer_constructor(self):
        element = HTMLElement.footer()
        self.assertEqual(element.tag_name, 'footer')
        self.assertFalse(element.is_self_closing)

    def test_form_constructor(self):
        element = HTMLElement.form()
        self.assertEqual(element.tag_name, 'form')
        self.assertFalse(element.is_self_closing)

    def test_h1_constructor(self):
        element = HTMLElement.h1()
        self.assertEqual(element.tag_name, 'h1')
        self.assertFalse(element.is_self_closing)

    def test_h2_constructor(self):
        element = HTMLElement.h2()
        self.assertEqual(element.tag_name, 'h2')
        self.assertFalse(element.is_self_closing)

    def test_h3_constructor(self):
        element = HTMLElement.h3()
        self.assertEqual(element.tag_name, 'h3')
        self.assertFalse(element.is_self_closing)

    def test_h4_constructor(self):
        element = HTMLElement.h4()
        self.assertEqual(element.tag_name, 'h4')
        self.assertFalse(element.is_self_closing)

    def test_h5_constructor(self):
        element = HTMLElement.h5()
        self.assertEqual(element.tag_name, 'h5')
        self.assertFalse(element.is_self_closing)

    def test_h6_constructor(self):
        element = HTMLElement.h6()
        self.assertEqual(element.tag_name, 'h6')
        self.assertFalse(element.is_self_closing)

    def test_head_constructor(self):
        element = HTMLElement.head()
        self.assertEqual(element.tag_name, 'head')
        self.assertFalse(element.is_self_closing)

    def test_header_constructor(self):
        element = HTMLElement.header()
        self.assertEqual(element.tag_name, 'header')
        self.assertFalse(element.is_self_closing)

    def test_hgroup_constructor(self):
        element = HTMLElement.hgroup()
        self.assertEqual(element.tag_name, 'hgroup')
        self.assertFalse(element.is_self_closing)

    def test_hr_constructor(self):
        element = HTMLElement.hr()
        self.assertEqual(element.tag_name, 'hr')
        self.assertTrue(element.is_self_closing)

    def test_html_constructor(self):
        element = HTMLElement.html()
        self.assertEqual(element.tag_name, 'html')
        self.assertFalse(element.is_self_closing)

    def test_i_constructor(self):
        element = HTMLElement.i()
        self.assertEqual(element.tag_name, 'i')
        self.assertFalse(element.is_self_closing)

    def test_iframe_constructor(self):
        element = HTMLElement.iframe()
        self.assertEqual(element.tag_name, 'iframe')
        self.assertFalse(element.is_self_closing)

    def test_img_constructor(self):
        element = HTMLElement.img()
        self.assertEqual(element.tag_name, 'img')
        self.assertTrue(element.is_self_closing)

    def test_input_constructor(self):
        element = HTMLElement.input()
        self.assertEqual(element.tag_name, 'input')
        self.assertTrue(element.is_self_closing)

    def test_ins_constructor(self):
        element = HTMLElement.ins()
        self.assertEqual(element.tag_name, 'ins')
        self.assertFalse(element.is_self_closing)

    def test_kbd_constructor(self):
        element = HTMLElement.kbd()
        self.assertEqual(element.tag_name, 'kbd')
        self.assertFalse(element.is_self_closing)

    def test_keygen_constructor(self):
        element = HTMLElement.keygen()
        self.assertEqual(element.tag_name, 'keygen')
        self.assertFalse(element.is_self_closing)

    def test_label_constructor(self):
        element = HTMLElement.label()
        self.assertEqual(element.tag_name, 'label')
        self.assertFalse(element.is_self_closing)

    def test_legend_constructor(self):
        element = HTMLElement.legend()
        self.assertEqual(element.tag_name, 'legend')
        self.assertFalse(element.is_self_closing)

    def test_li_constructor(self):
        element = HTMLElement.li()
        self.assertEqual(element.tag_name, 'li')
        self.assertFalse(element.is_self_closing)

    def test_link_constructor(self):
        element = HTMLElement.link()
        self.assertEqual(element.tag_name, 'link')
        self.assertTrue(element.is_self_closing)

    def test_main_constructor(self):
        element = HTMLElement.main()
        self.assertEqual(element.tag_name, 'main')
        self.assertFalse(element.is_self_closing)

    def test_map_constructor(self):
        element = HTMLElement.map()
        self.assertEqual(element.tag_name, 'map')
        self.assertFalse(element.is_self_closing)

    def test_mark_constructor(self):
        element = HTMLElement.mark()
        self.assertEqual(element.tag_name, 'mark')
        self.assertFalse(element.is_self_closing)

    def test_math_constructor(self):
        element = HTMLElement.math()
        self.assertEqual(element.tag_name, 'math')
        self.assertFalse(element.is_self_closing)

    def test_menu_constructor(self):
        element = HTMLElement.menu()
        self.assertEqual(element.tag_name, 'menu')
        self.assertFalse(element.is_self_closing)

    def test_menuitem_constructor(self):
        element = HTMLElement.menuitem()
        self.assertEqual(element.tag_name, 'menuitem')
        self.assertFalse(element.is_self_closing)

    def test_meta_constructor(self):
        element = HTMLElement.meta()
        self.assertEqual(element.tag_name, 'meta')
        self.assertTrue(element.is_self_closing)

    def test_meter_constructor(self):
        element = HTMLElement.meter()
        self.assertEqual(element.tag_name, 'meter')
        self.assertFalse(element.is_self_closing)

    def test_nav_constructor(self):
        element = HTMLElement.nav()
        self.assertEqual(element.tag_name, 'nav')
        self.assertFalse(element.is_self_closing)

    def test_noscript_constructor(self):
        element = HTMLElement.noscript()
        self.assertEqual(element.tag_name, 'noscript')
        self.assertFalse(element.is_self_closing)

    def test_object_constructor(self):
        element = HTMLElement.object()
        self.assertEqual(element.tag_name, 'object')
        self.assertFalse(element.is_self_closing)

    def test_ol_constructor(self):
        element = HTMLElement.ol()
        self.assertEqual(element.tag_name, 'ol')
        self.assertFalse(element.is_self_closing)

    def test_optgroup_constructor(self):
        element = HTMLElement.optgroup()
        self.assertEqual(element.tag_name, 'optgroup')
        self.assertFalse(element.is_self_closing)

    def test_option_constructor(self):
        element = HTMLElement.option()
        self.assertEqual(element.tag_name, 'option')
        self.assertFalse(element.is_self_closing)

    def test_output_constructor(self):
        element = HTMLElement.output()
        self.assertEqual(element.tag_name, 'output')
        self.assertFalse(element.is_self_closing)

    def test_p_constructor(self):
        element = HTMLElement.p()
        self.assertEqual(element.tag_name, 'p')
        self.assertFalse(element.is_self_closing)

    def test_param_constructor(self):
        element = HTMLElement.param()
        self.assertEqual(element.tag_name, 'param')
        self.assertTrue(element.is_self_closing)

    def test_picture_constructor(self):
        element = HTMLElement.picture()
        self.assertEqual(element.tag_name, 'picture')
        self.assertFalse(element.is_self_closing)

    def test_pre_constructor(self):
        element = HTMLElement.pre()
        self.assertEqual(element.tag_name, 'pre')
        self.assertFalse(element.is_self_closing)

    def test_progress_constructor(self):
        element = HTMLElement.progress()
        self.assertEqual(element.tag_name, 'progress')
        self.assertFalse(element.is_self_closing)

    def test_q_constructor(self):
        element = HTMLElement.q()
        self.assertEqual(element.tag_name, 'q')
        self.assertFalse(element.is_self_closing)

    def test_rb_constructor(self):
        element = HTMLElement.rb()
        self.assertEqual(element.tag_name, 'rb')
        self.assertFalse(element.is_self_closing)

    def test_rp_constructor(self):
        element = HTMLElement.rp()
        self.assertEqual(element.tag_name, 'rp')
        self.assertFalse(element.is_self_closing)

    def test_rt_constructor(self):
        element = HTMLElement.rt()
        self.assertEqual(element.tag_name, 'rt')
        self.assertFalse(element.is_self_closing)

    def test_rtc_constructor(self):
        element = HTMLElement.rtc()
        self.assertEqual(element.tag_name, 'rtc')
        self.assertFalse(element.is_self_closing)

    def test_ruby_constructor(self):
        element = HTMLElement.ruby()
        self.assertEqual(element.tag_name, 'ruby')
        self.assertFalse(element.is_self_closing)

    def test_s_constructor(self):
        element = HTMLElement.s()
        self.assertEqual(element.tag_name, 's')
        self.assertFalse(element.is_self_closing)

    def test_samp_constructor(self):
        element = HTMLElement.samp()
        self.assertEqual(element.tag_name, 'samp')
        self.assertFalse(element.is_self_closing)

    def test_script_constructor(self):
        element = HTMLElement.script()
        self.assertEqual(element.tag_name, 'script')
        self.assertFalse(element.is_self_closing)

    def test_section_constructor(self):
        element = HTMLElement.section()
        self.assertEqual(element.tag_name, 'section')
        self.assertFalse(element.is_self_closing)

    def test_select_constructor(self):
        element = HTMLElement.select()
        self.assertEqual(element.tag_name, 'select')
        self.assertFalse(element.is_self_closing)

    def test_slot_constructor(self):
        element = HTMLElement.slot()
        self.assertEqual(element.tag_name, 'slot')
        self.assertFalse(element.is_self_closing)

    def test_small_constructor(self):
        element = HTMLElement.small()
        self.assertEqual(element.tag_name, 'small')
        self.assertFalse(element.is_self_closing)

    def test_source_constructor(self):
        element = HTMLElement.source()
        self.assertEqual(element.tag_name, 'source')
        self.assertTrue(element.is_self_closing)

    def test_span_constructor(self):
        element = HTMLElement.span()
        self.assertEqual(element.tag_name, 'span')
        self.assertFalse(element.is_self_closing)

    def test_strong_constructor(self):
        element = HTMLElement.strong()
        self.assertEqual(element.tag_name, 'strong')
        self.assertFalse(element.is_self_closing)

    def test_style_constructor(self):
        element = HTMLElement.style()
        self.assertEqual(element.tag_name, 'style')
        self.assertFalse(element.is_self_closing)

    def test_sub_constructor(self):
        element = HTMLElement.sub()
        self.assertEqual(element.tag_name, 'sub')
        self.assertFalse(element.is_self_closing)

    def test_summary_constructor(self):
        element = HTMLElement.summary()
        self.assertEqual(element.tag_name, 'summary')
        self.assertFalse(element.is_self_closing)

    def test_sup_constructor(self):
        element = HTMLElement.sup()
        self.assertEqual(element.tag_name, 'sup')
        self.assertFalse(element.is_self_closing)

    def test_svg_constructor(self):
        element = HTMLElement.svg()
        self.assertEqual(element.tag_name, 'svg')
        self.assertFalse(element.is_self_closing)

    def test_table_constructor(self):
        element = HTMLElement.table()
        self.assertEqual(element.tag_name, 'table')
        self.assertFalse(element.is_self_closing)

    def test_tbody_constructor(self):
        element = HTMLElement.tbody()
        self.assertEqual(element.tag_name, 'tbody')
        self.assertFalse(element.is_self_closing)

    def test_td_constructor(self):
        element = HTMLElement.td()
        self.assertEqual(element.tag_name, 'td')
        self.assertFalse(element.is_self_closing)

    def test_template_constructor(self):
        element = HTMLElement.template()
        self.assertEqual(element.tag_name, 'template')
        self.assertFalse(element.is_self_closing)

    def test_textarea_constructor(self):
        element = HTMLElement.textarea()
        self.assertEqual(element.tag_name, 'textarea')
        self.assertFalse(element.is_self_closing)

    def test_tfoot_constructor(self):
        element = HTMLElement.tfoot()
        self.assertEqual(element.tag_name, 'tfoot')
        self.assertFalse(element.is_self_closing)

    def test_th_constructor(self):
        element = HTMLElement.th()
        self.assertEqual(element.tag_name, 'th')
        self.assertFalse(element.is_self_closing)

    def test_thead_constructor(self):
        element = HTMLElement.thead()
        self.assertEqual(element.tag_name, 'thead')
        self.assertFalse(element.is_self_closing)

    def test_time_constructor(self):
        element = HTMLElement.time()
        self.assertEqual(element.tag_name, 'time')
        self.assertFalse(element.is_self_closing)

    def test_title_constructor(self):
        element = HTMLElement.title()
        self.assertEqual(element.tag_name, 'title')
        self.assertFalse(element.is_self_closing)

    def test_tr_constructor(self):
        element = HTMLElement.tr()
        self.assertEqual(element.tag_name, 'tr')
        self.assertFalse(element.is_self_closing)

    def test_track_constructor(self):
        element = HTMLElement.track()
        self.assertEqual(element.tag_name, 'track')
        self.assertTrue(element.is_self_closing)

    def test_u_constructor(self):
        element = HTMLElement.u()
        self.assertEqual(element.tag_name, 'u')
        self.assertFalse(element.is_self_closing)

    def test_ul_constructor(self):
        element = HTMLElement.ul()
        self.assertEqual(element.tag_name, 'ul')
        self.assertFalse(element.is_self_closing)

    def test_var_constructor(self):
        element = HTMLElement.var()
        self.assertEqual(element.tag_name, 'var')
        self.assertFalse(element.is_self_closing)

    def test_video_constructor(self):
        element = HTMLElement.video()
        self.assertEqual(element.tag_name, 'video')
        self.assertFalse(element.is_self_closing)

    def test_wbr_constructor(self):
        element = HTMLElement.wbr()
        self.assertEqual(element.tag_name, 'wbr')
        self.assertTrue(element.is_self_closing)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
