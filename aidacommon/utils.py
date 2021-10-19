from collections import OrderedDict;
import re;

class VirtualOrderedColumnsDict(OrderedDict):
    def __init__(self, numkeys=0, dataProvider=None, numformatter='{:010d}', start=0, colprefix='', colsuffix=''):
        super().__init__();
        self.__numkeys__ = numkeys;
        self.__dataProvider__ = dataProvider;
        self.__numformatter__ = numformatter;
        self.__start__ = start;
        self.__colprefix__ = colprefix;
        self.__colsuffix__ = colsuffix;

    @property
    def numformatter(self):
        return self.__numformatter__;

    def __getitem__(self, key):
        #Extract the numerical equivalent of the key position.
        keyno = int(re.sub(self.__colsuffix__+'$', '', re.sub('^'+self.__colprefix__, '', key)));
        #If this is not with in the valid range, throw an error.
        if(keyno < self.__start__ or keyno >= self.__start__+self.__numkeys__):
            raise KeyError(key);

        try:
            data = super().__getitem__(keyno);
        except KeyError:
            data = self.__dataProvider__.get(keyno)
            super().__setitem__(keyno, data);
        return  data;

    def __len__(self):
        return self.__numkeys__;

    class VirtualOrderedColumnsIterator:
        def __init__(self, numkeys, numformatter='{:010d}', start=0, colprefix='', colsuffix=''):
            self.__numkeys__ = numkeys;
            self.__numformatter__ = numformatter;
            self.__start__ = start;
            self.__colprefix__ = colprefix;
            self.__colsuffix__ = colsuffix;
            self.__current__ = None;
            pass;

        def __next__(self):
            if(self.__current__ is None):
                self.__current__ = self.__start__;
            elif(self.__current__ == self.__start__ + self.__numkeys__ ):
                raise StopIteration();

            item =  ('{}'+self.__numformatter__+'{}').format(self.__colprefix__, self.__current__, self.__colsuffix__);
            self.__current__ += 1;
            return item;

    def __iter__(self):
        return VirtualOrderedColumnsDict.VirtualOrderedColumnsIterator(self.__numkeys__, self.__numformatter__, self.__start__, self.__colprefix__, self.__colsuffix__);

    class VirtualOrderedColumns:
        def __init__(self, numkeys, numformatter='{:010d}', start=0, colprefix='', colsuffix=''):
            self.__numkeys__ = numkeys;
            self.__numformatter__ = numformatter;
            self.__start__ = start;
            self.__colprefix__ = colprefix;
            self.__colsuffix__ = colsuffix;

        def __iter__(self):
            return VirtualOrderedColumnsDict.VirtualOrderedColumnsIterator(self.__numkeys__, self.__numformatter__, self.__start__, self.__colprefix__, self.__colsuffix__);

        def __len__(self):
            return self.__numkeys__;


    def keys(self):
        return VirtualOrderedColumnsDict.VirtualOrderedColumns(self.__numkeys__, self.__numformatter__, self.__start__, self.__colprefix__, self.__colsuffix__);

def matchExp(str1, str2):
    pass
