import unittest

from da.freeze import frozendict, frozenlist, deepfreeze


class TestFrozenDictList(unittest.TestCase):
    def setUp(self):
        self.d = {1: "a"}
        self.l = [1, 2, 3]

    def test_frozendict(self):
        fd = frozendict(self.d)
        self.assertTrue(1 in fd)
        self.assertEqual(fd[1], "a")
        with self.assertRaises(AttributeError):
            fd[1] = "b"
        with self.assertRaises(AttributeError):
            fd[2] = "a"
        with self.assertRaises(AttributeError):
            fd.pop(1)
        with self.assertRaises(AttributeError):
            fd.popitem()
        with self.assertRaises(AttributeError):
            fd.setdefault(2)
        with self.assertRaises(AttributeError):
            fd.update({1 : "b", 2 : "a"})
        with self.assertRaises(AttributeError):
            del fd[1]
        self.assertEqual(fd, {1 : "a"})
        self.assertEqual(hash(fd), hash(frozendict(self.d)))

    def test_frozenlist(self):
        fl = frozenlist(self.l)
        self.assertEqual(len(fl), len(self.l))
        self.assertEqual(fl, [1, 2, 3])
        with self.assertRaises(AttributeError):
            fl.append(4)
        with self.assertRaises(AttributeError):
            fl.extend([4, 5])
        with self.assertRaises(AttributeError):
            fl += (1, 2)
        with self.assertRaises(AttributeError):
            fl *= 4
        self.assertEqual(fl + [1, 2], [1, 2, 3, 1, 2])
        self.assertEqual(fl * 2, [1, 2, 3, 1, 2, 3])
        self.assertEqual(len({fl}), 1)
        self.assertTrue(fl in {fl})


class UserObj:
    def __init__(self):
        self.f0 = [1]
        self.f1 = {10}
        self.f2 = {1 : 2, 2 : self.f1}

    def __eq__(self, other):
        return isinstance(other, type(self)) and \
            self.f0 == other.f0 and \
            self.f1 == other.f1 and \
            self.f2 == other.f2

class TestDeepFreeze(unittest.TestCase):
    def setUp(self):
        self.s = {1, 2, 0}
        self.l = [1, 3, 2]
        self.rl = [10, 9, self.s, self.s]
        self.rl.append(self.rl)
        self.d = {"a" : 1, ("b", 0) : self.l}
        self.rd = {1 : self.rl}
        self.rd["self"] = self.rd

    def test_freeze_set(self):
        fs = deepfreeze(self.s)
        self.assertTrue(isinstance(fs, frozenset))
        self.assertEqual(fs, self.s)

    def test_freeze_list(self):
        fl = deepfreeze(self.l)
        self.assertTrue(isinstance(fl, frozenlist))
        self.assertEqual(self.l, fl)
        with self.assertRaises(AttributeError):
            fl.append(4)
        fl = deepfreeze(self.rl)
        self.assertIs(fl[-1], fl)
        self.assertIs(fl[3], fl[2])
        with self.assertRaises(AttributeError):
            fl.update(self.l)

    def test_freeze_dict(self):
        fd = deepfreeze(self.d)
        self.assertTrue(isinstance(fd, frozendict))
        fd = deepfreeze(self.rd)
        self.assertTrue(isinstance(fd, frozendict))

    def test_freeze_object(self):
        obj = UserObj()
        fobj = deepfreeze(obj)
        self.assertTrue(isinstance(fobj, UserObj))
        self.assertFalse(obj is fobj)
        self.assertEqual(obj, fobj)
        with self.assertRaises(AttributeError):
            fobj.f0.insert(1, 2)
        with self.assertRaises(AttributeError):
            fobj.f2[1] = 3


if __name__ == '__main__':
    unittest.main()
