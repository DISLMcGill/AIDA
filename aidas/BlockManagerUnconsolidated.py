from pandas.core.internals import BlockManager
import pandas as pd

class BlockManagerUnconsolidated(BlockManager):
    def __init__(self, *args, **kwargs):
        BlockManager.__init__(self, *args, **kwargs)
        self._is_consolidated = False
        self._known_consolidated = False

    def _consolidate_inplace(self): pass
    def _consolidate(self): return self.blocks


def df_from_arrays(arrays, columns, index):
    from pandas.core.internals import make_block
    def gen():
        p = 0
        for val in arrays:
            _len = len(val)
            yield make_block(values=val.reshape((1,_len)), placement=(p,))
            p+=1

    blocks = tuple(gen())
    mgr = BlockManagerUnconsolidated(blocks=blocks, axes=[columns, index])
    return pd.DataFrame(mgr, copy=False)
