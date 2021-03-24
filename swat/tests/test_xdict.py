#!/usr/bin/env python
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import copy
import swat.utils.testing as tm
import unittest
from swat.utils.xdict import xdict, xadict

out1 = {
    'a': {'one': 1},
    'b': {'two': 2},
    'c': {
        'three': {'nest': 3},
        'four': {
            'nest': {'double': 4}
        }
    }
}

flatout1 = {
    'a.one': 1,
    'b.two': 2,
    'c.three.nest': 3,
    'c.four.nest.double': 4
}


class TestXDict(tm.TestCase):

    def test_constructor(self):
        x = xdict(one=1, two=2, three=3)
        self.assertEqual(x, dict(one=1, two=2, three=3))

        x = xdict([('a.one', 1), ('b.two', 2), ('c.three.nest', 3)])
        self.assertEqual(x, {
            'a': {'one': 1},
            'b': {'two': 2},
            'c': {
                'three': {'nest': 3}
            }
        })

        x = xdict([('a.one', 1), ('b.two', 2), ('c.three.nest', 3),
                   ('c.four.nest.double', 4)])
        self.assertEqual(x, out1)

        x = xdict(out1)
        self.assertEqual(x, out1)

        x = xdict(flatout1)
        self.assertEqual(x, out1)

        x = xdict(**out1)
        self.assertEqual(x, out1)

        x = xdict(**flatout1)
        self.assertEqual(x, out1)

    def test_setitem(self):
        x = xdict()
        x['a.one'] = 1
        x['b.two'] = 2
        x['c.three.nest'] = 3
        x['c.four.nest.double'] = 4
        self.assertEqual(x, out1)

        x[100] = 1000
        self.assertEqual(x[100], 1000)

        x = xdict()
        x['a.one'] = 1
        x['b.two'] = 2
        x['c.three.nest'] = 3
        x['c.four'] = {'nest': {'double': 4}}
        self.assertEqual(x, out1)

    def test_getitem(self):
        x = xdict(out1)
        self.assertEqual(x['a.one'], 1)
        self.assertEqual(x['b.two'], 2)
        self.assertEqual(x['c.three.nest'], 3)
        self.assertEqual(x['c.four.nest.double'], 4)

        self.assertEqual(x['a'], {'one': 1})
        self.assertEqual(x['b'], {'two': 2})
        self.assertEqual(x['c'], {'three': {'nest': 3}, 'four': {'nest': {'double': 4}}})
        self.assertEqual(x['c.three'], {'nest': 3})
        self.assertEqual(x['c.four'], {'nest': {'double': 4}})
        self.assertEqual(x['c.four.nest'], {'double': 4})

        self.assertEqual(x['a'], {'one': 1})
        self.assertEqual(x['a']['one'], 1)
        self.assertEqual(x['b'], {'two': 2})
        self.assertEqual(x['b']['two'], 2)
        self.assertEqual(x['c'], {'three': {'nest': 3}, 'four': {'nest': {'double': 4}}})
        self.assertEqual(x['c']['three'], {'nest': 3})
        self.assertEqual(x['c']['three']['nest'], 3)
        self.assertEqual(x['c']['four'], {'nest': {'double': 4}})
        self.assertEqual(x['c']['four']['nest'], {'double': 4})
        self.assertEqual(x['c']['four']['nest']['double'], 4)

    def test_get(self):
        x = xdict(out1)

        self.assertEqual(x.get('a', 1000), {'one': 1})
        self.assertEqual(x.get('a.one', 1000), 1)
        self.assertEqual(x.get('a.does.not.exist', 1000), 1000)

    def test_delitem(self):
        newout1 = copy.deepcopy(out1)
        x = xdict(out1)

        del x['a']
        del newout1['a']
        self.assertEqual(x, newout1)

        del x['c.four.nest']
        del newout1['c']['four']['nest']
        self.assertEqual(x, newout1)

        with self.assertRaises(KeyError):
            del x['a.does.not.exist']

    def test_flattened(self):
        x = xdict(out1)
        self.assertEqual(x.flattened(), flatout1)

    def test_setdefault(self):
        x = xdict(out1)

        self.assertEqual(x.setdefault('a.one'), 1)
        self.assertEqual(x.setdefault('b.two'), 2)
        self.assertEqual(x.setdefault('b'), {'two': 2})

        self.assertEqual(x.setdefault('w.none'), None)
        self.assertEqual(x['w.none'], None)
        self.assertEqual(x['w']['none'], None)
        self.assertEqual(x.setdefault('x.y.z', 200), 200)
        self.assertEqual(x['x.y.z'], 200)
        self.assertEqual(x['x']['y']['z'], 200)
        self.assertEqual(x.setdefault('x.y.z', 10), 200)
        self.assertEqual(x['x.y.z'], 200)
        self.assertEqual(x['x']['y']['z'], 200)
        self.assertEqual(x['x']['y'], {'z': 200})

        self.assertEqual(x.setdefault('does.not.exist', {'new': 'key'}), {'new': 'key'})
        self.assertTrue(isinstance(x.setdefault('does.not.exist', {'new': 'key'}), xdict))

    def test_json(self):
        x = xdict(out1)
        self.assertEqual(xdict.from_json(x.to_json()), out1)

    def test_flat(self):
        x = xdict(out1)

        self.assertEqual(sorted(x.flatkeys()), sorted(flatout1.keys()))
        self.assertEqual(sorted(x.flatvalues()), sorted(flatout1.values()))
        self.assertEqual(sorted(x.flatitems()), sorted(flatout1.items()))

        if hasattr(flatout1, 'iterkeys'):
            self.assertEqual(sorted(x.iterflatkeys()), sorted(flatout1.iterkeys()))
        if hasattr(flatout1, 'itervalues'):
            self.assertEqual(sorted(x.iterflatvalues()), sorted(flatout1.itervalues()))
        if hasattr(flatout1, 'iteritems'):
            self.assertEqual(sorted(x.iterflatitems()), sorted(flatout1.iteritems()))

        if hasattr(flatout1, 'iterkeys'):
            self.assertEqual(sorted(x.viewflatkeys()), sorted(flatout1.iterkeys()))
        if hasattr(flatout1, 'itervalues'):
            self.assertEqual(sorted(x.viewflatvalues()), sorted(flatout1.itervalues()))
        if hasattr(flatout1, 'iteritems'):
            self.assertEqual(sorted(x.viewflatitems()), sorted(flatout1.iteritems()))

    def test_contains(self):
        x = xdict(out1)

        self.assertTrue('a' in x)
        self.assertTrue('a.one' in x)
        self.assertTrue('b' in x)
        self.assertTrue('c.four' in x)
        self.assertTrue('c.four.nest' in x)
        self.assertTrue('c.four.nest.double' in x)
        self.assertFalse('z' in x)
        self.assertFalse('four' in x)

        self.assertTrue(x.has_key('a'))  # noqa: W601
        self.assertTrue(x.has_key('a.one'))  # noqa: W601
        self.assertTrue(x.has_key('b'))  # noqa: W601
        self.assertTrue(x.has_key('c.four'))  # noqa: W601
        self.assertTrue(x.has_key('c.four.nest'))  # noqa: W601
        self.assertTrue(x.has_key('c.four.nest.double'))  # noqa: W601
        self.assertFalse(x.has_key('z'))  # noqa: W601
        self.assertFalse(x.has_key('four'))  # noqa: W601

    def test_pop(self):
        x = xdict(out1)

        self.assertEqual(x, out1)

        self.assertEqual(x.pop('a.one'), out1['a']['one'])
        self.assertEqual(x.pop('b.two'), out1['b']['two'])
        self.assertEqual(x.pop('c'), out1['c'])

        self.assertEqual(x, {'a': {}, 'b': {}})

        with self.assertRaises(KeyError):
            x.pop('a.does.not.exist')

        self.assertEqual(x.pop('a.does.not.exist', 1000), 1000)

    def test_copy(self):
        w = xdict(out1)

        x = w.copy()

        self.assertEqual(w, x)
        self.assertTrue(w is not x)
        self.assertTrue(w['a'] is x['a'])
        self.assertTrue(w['b'] is x['b'])
        self.assertTrue(w['c'] is x['c'])

        y = copy.copy(w)

        self.assertEqual(w, y)
        self.assertTrue(w is not y)
        self.assertTrue(w['a'] is y['a'])
        self.assertTrue(w['a.one'] is y['a.one'])
        self.assertTrue(w['b'] is y['b'])
        self.assertTrue(w['b.two'] is y['b.two'])
        self.assertTrue(w['c'] is y['c'])
        self.assertTrue(w['c.three'] is y['c.three'])

        z = copy.deepcopy(w)

        # TODO: Deepcopy is only shallow copying
        self.assertEqual(w, z)
        self.assertTrue(w is not z)
        self.assertTrue(w['a'] is not z['a'])
#       self.assertTrue(w['a.one'] is not z['a.one'])
        self.assertTrue(w['b'] is not z['b'])
#       self.assertTrue(w['b.two'] is not z['b.two'])
        self.assertTrue(w['c'] is not z['c'])
#       self.assertTrue(w['c.three'] is not z['c.three'])

    def test_attrs(self):
        x = xadict(out1)

        self.assertEqual(x.a, x['a'])
        self.assertEqual(x.a.one, x['a.one'])
        self.assertEqual(x.a.one, x['a']['one'])
        self.assertEqual(x.b, x['b'])
        self.assertEqual(x.b.two, x['b.two'])
        self.assertEqual(x.b.two, x['b']['two'])
        self.assertEqual(x.c, x['c'])
        self.assertEqual(x.c.three, x['c.three'])
        self.assertEqual(x.c.three, x['c']['three'])

        x.a.one = 100
        x.c.four.nest = {'float': 5}

        self.assertEqual(x['a.one'], 100)
        self.assertEqual(x['c.four.nest'], {'float': 5})

        del x.c.three

        self.assertEqual(x.c, {'four': {'nest': {'float': 5}}})

        del x.c

        self.assertEqual(x, {'a': {'one': 100}, 'b': {'two': 2}})

        del x.a
        del x.b

        self.assertEqual(x, {})

        # Non-existent keys
        x.foo.bar.xxx = 10
        x.foo.baz = 'hi there'

        self.assertEqual(x, {'foo': {'bar': {'xxx': 10}, 'baz': 'hi there'}})

    def test_ints(self):
        x = xadict(out1)

        out1int = dict(out1)
        out1int['d'] = {}
        out1int['d'][0] = dict(hi='there', bye='now')
        out1int['d'][1] = dict(test='value')
        out1int['d'][2] = 100

        x.d[0].hi = 'there'
        x.d[0].bye = 'now'
        x.d[1].test = 'value'
        x.d[2] = 100

        self.assertEqual(x, out1int)

        # Flat
        flatout1int = dict(flatout1)
        flatout1int['d[0].hi'] = 'there'
        flatout1int['d[0].bye'] = 'now'
        flatout1int['d[1].test'] = 'value'
        flatout1int['d[2]'] = 100

        self.assertEqual(x.flattened(), flatout1int)


if __name__ == '__main__':
    from swat.utils.testing import runtests
    runtests()
