from da.compiler.dast import DistNode

class DARules(DistNode): 
    _fields = []

    def __init__(self, *args):
        """Populate fields named in "fields" with values in *args."""
        # print('fields:', self._fields)
        # print('args:', args)
        assert(len(self._fields) == len(args))
        for f, a in zip(self._fields, args):
            setattr(self, f, a)

class LogicVar (DARules):
    _fields = ['name']

class Constant (DARules):
    _fields = ['name']

class Assertion (DARules):
    _fields = ['pred', 'args']

class Rule (DARules):
    _fields = ['concl', 'conds']

    def __init__(self, concl, conds=None): 
        super().__init__(concl, conds)



class Rules (DARules):
    _fields = ['decls', 'rules']

class InferStmt(DistNode): #dast.Statement
    _fields = ['bindings', 'queries', 'rule']

    def __init__(self, parent, ast=None): 
        super().__init__(parent, ast)
        self.bindings = []
        self.queries = []
        self.rule = 'tmp'

