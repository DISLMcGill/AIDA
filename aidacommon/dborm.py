import logging;
from enum import Enum;
from abc import ABCMeta, abstractmethod, abstractproperty;

import datetime;

import numpy as np;

#Enumeration to keep track of join keywords (including syntactic sugar)
class JOIN(Enum):
    INNER='INNER JOIN'; INNER_JOIN='INNER JOIN'; JOIN='INNER JOIN';
    OUTER='FULL OUTER JOIN'; FULL_OUTER='FULL OUTER JOIN'; FULL_OUTER_JOIN='FULL OUTER JOIN';
    LEFT='LEFT OUTER JOIN';  LEFT_OUTER='LEFT OUTER JOIN'; LEFT_OUTER_JOIN='LEFT OUTER JOIN';
    RIGHT='RIGHT OUTER JOIN'; RIGHT_OUTER='RIGHT OUTER JOIN'; RIGHT_OUTER_JOIN='RIGHT OUTER JOIN';
    CROSS_JOIN='CROSS JOIN';

#Enumeration to indicate if all columns are projected from a given table (for a join)
class COL(Enum):
    NONE=1
    ALL=2;


class CMP(Enum):
    EQUAL='=';EQ='='; NOTEQUAL='<>';NE='<>';
    GREATERTHAN='>';GT='>'; GTALL='> ALL'; GTANY='> ANY'; GREATERTHANOREQUAL='>=';GTE='>='; GTEALL='>= ALL'; GTEANY='>= ANY';
    LESSTHAN='<';LT='<'; LTALL='< ALL'; LTANY='< ANY'; LESSTHANOREQUAL='<=';LTE='<='; LTEALL='<= ALL'; LTEANY='<= ANY';
    IN='IN'; NOTIN='NOT IN'; EXISTS='EXISTS'; NOTEXISTS='NOT EXISTS';
    LIKE='LIKE'; NOTLIKE='NOT LIKE';
    OR='OR'; AND='AND';
    NULL='IS NULL';NL='IS NULL'; NOTNULL='IS NOT NULL';NNL='IS NOT NULL';
    NOT='NOT';

CMP.binaryOps = {CMP.EQUAL, CMP.NOTEQUAL
                , CMP.GREATERTHAN, CMP.GTALL, CMP.GTANY
                , CMP.GREATERTHANOREQUAL, CMP.GTEALL, CMP.GTEANY
                , CMP.LESSTHAN, CMP.LTALL, CMP.LTANY
                , CMP.LESSTHANOREQUAL, CMP.LTEALL, CMP.LTEANY
                , CMP.LIKE, CMP.NOTLIKE, CMP.OR, CMP.AND};
CMP.subqueryOps = {CMP.IN, CMP.NOTIN, CMP.EXISTS, CMP.NOTEXISTS};
CMP.unaryOps = {CMP.NULL, CMP.NOTNULL, CMP.NOT};

class OP(Enum):
    ADD='+'; SUBTRACT='-'; MULTIPLY='*'; DIVIDE='/'; NEGATIVE='(-)'; EXP='**'; MATRIXMULTIPLY='@'; TRANSPOSE='T';
    LHS='LHS'; RHS='RHS';

#TODO: Add numpy.* data types for all scalar isinstance checks on ints and floats.

class AIDADtypes(metaclass=ABCMeta):
    integers = (int, np.int8, np.int16, np.int32, np.int64);
    decimals = (float, np.float16, np.float32, np.float64, np.float128);
    numeric = integers + decimals;

    @classmethod
    def formatnumeric(self, val):
        if type(val) in AIDADtypes.integers:
            return '{}'.format(val);
        elif type(val) in AIDADtypes.decimals:
            return 'CAST({} AS FLOAT)'.format(val);
        raise TypeError('Error type {} is not supported'.format(type(val)));


#Class to hold constants in expressions of columns used in projections.
class C:
    def __init__(self, val):
        self._val_ = val;
    def __format__(self, format_spec):
        #if(isinstance(self._val_, int) or isinstance(self._val_,float)):
        if(type(self._val_) in AIDADtypes.numeric):
            return AIDADtypes.formatnumeric(self._val_);
            #return '{}'.format(self._val_);
        if(isinstance(self._val_, str)):
            return '\'{}\''.format(self._val_);
    def __str__(self):
        return '{}'.format(self._val_);
    #Form the SQL text for this constant
    @property
    def columnExpr(self):
        return str(self);

class DATE:
    def __init__(self, dte):
        self.__date__ = datetime.datetime.strptime(dte, '%Y-%m-%d');
    def __format__(self, format_spec):
        return self.__date__.strftime('\'%Y-%m-%d\'');
    def __str__(self):
        return self.__date__.strftime('\'%Y-%m-%d\'');
    def __repr__(self):
        return self.__date__.strftime('\'%Y-%m-%d\'');

#Class to hold column names / expressions in selections.
class Q:

    #Enumeration for operations supported by this class.
    class OP(Enum):
        ADD='+'; SUBTRACT='-'; MULTIPLY='*'; DIVIDE='/'; NEGATIVE='(-)';

    @classmethod
    def __formatval__(cls, val, checkstr=False):
        if(isinstance(val, TabularData)):
            return '(' + val.genSQL.sqlText + ')';
        if(isinstance(val, Q)):
            return val.columnExpr;
        #if(isinstance(val, int) or isinstance(val, float) or isinstance(val, C)) :
        #if(type(val) in (C,) or type(val) in AIDADtypes.numeric):
        if(type(val) in (C,)):
            return '{}'.format(val);
        if(type(val) in AIDADtypes.numeric):
            return AIDADtypes.formatnumeric(val);
        if(isinstance(val, DATE)):
            return str(val);
        if(checkstr and isinstance(val, str)):
            return '\'{}\''.format(val);
        return val;

    def __init__(self, col1=None, col2=None, operator=CMP.EQ):
        self._col1_ = col1;
        if(not (operator==CMP.EQ and col2 is None)):
            self._col2_ = col2;
            self._operator_ = operator;
        else:
            self._col2_ = self._operator_ = None;

    #Get a list of source columns referenced in this expression.
    @property
    def srcColList(self):
        scl = [];
        if(hasattr(self._col1_, 'srcColList')):
            scl += self._col1_.srcColList;
        if(hasattr(self._col2_, 'srcColList')):
            scl += self._col2_.srcColList;
        if(isinstance(self._col1_, str)):
            scl.append(self._col1_);
        return scl;

    #Form the SQL text for this column / expression
    @property
    def columnExpr(self):
        if(not self._operator_): #If this is not an expression, it is just the column.
            return self.__formatval__(self._col1_);
        if(self._operator_== Q.OP.NEGATIVE):  #Unary negative
            return '(' + '-'  + Q.__formatval__(self._col1_) + ')';
        if(self._operator_ == CMP.NOT):
            return  '(' + 'NOT '  + Q.__formatval__(self._col1_) + ')';
        #if(self._operator_ in [CMP.IN, CMP.NOTIN]):
        if(self._operator_ in CMP.subqueryOps):
            if(hasattr(self._col2_, 'genSQL')):
                return '(' +  Q.__formatval__(self._col1_) + ' ' + self._operator_.value + ' ' +  '('+  ((self._col2_.genSQL.sqlText)if(hasattr(self._col2_.genSQL, 'sqlText'))else(self._col2_.genSQL)) +')'  + ')';
            else:
                inlist='';
                for v in self._col2_:
                    inlist = ((inlist+',') if(inlist) else '') + self.__formatval__(v, True);
                return '(' +  Q.__formatval__(self._col1_) + ' ' + self._operator_.value + ' ' +  '('+inlist+')'  + ')';
        if(self._operator_ in CMP.unaryOps):
            return  '(' +  Q.__formatval__(self._col1_) + ' ' + self._operator_.value + ' ' + ')';
        if(self._operator_ in Q.binaryOps or self._operator_ in CMP.binaryOps): #If this is an expression with a binary operator, generate a combined expression.
            return  '(' + Q.__formatval__(self._col1_) + ' ' + self._operator_.value + ' ' + Q.__formatval__(self._col2_) + ')';

    # Methods to support linear algebra operations on columns.
    def __add__(self, other):
        return Q(self, col2=other, operator=Q.OP.ADD);

    def __radd__(self, other):
        return Q(other, col2=self, operator=Q.OP.ADD);

    def __sub__(self, other):
        return Q(self, col2=other, operator=Q.OP.SUBTRACT);

    def __rsub__(self, other):
        return Q(other, col2=self, operator=Q.OP.SUBTRACT);

    def __mul__(self, other):
        return Q(self, col2=other, operator=Q.OP.MULTIPLY);

    def __rmul__(self, other):
        return Q(other, col2=self, operator=Q.OP.MULTIPLY);

    def __truediv__(self, other):
        return Q(self, col2=other, operator=Q.OP.DIVIDE);

    def __rtruediv__(self, other):
        return Q(other, col2=self, operator=Q.OP.DIVIDE);

    def __neg__(self):
        return Q(self, operator=Q.OP.NEGATIVE);

    def __and__(self, other):
        if(not isinstance(other, Q)):
            raise NotImplemented;
        return Q(self, col2=other, operator=CMP.AND);

    def __or__(self, other):
        if(not isinstance(other, Q)):
            raise NotImplemented;
        return Q(self, col2=other, operator=CMP.OR);

    def __invert__(self):
        return Q(self, operator=CMP.NOT);

#List of binary operators.
Q.binaryOps = (Q.OP.ADD, Q.OP.SUBTRACT, Q.OP.MULTIPLY, Q.OP.DIVIDE);



#Class to hold column names / expressions using them in projections.
class F:

    #Enumeration for operations supported by this class.
    class OP(Enum):
        ADD='+'; SUBTRACT='-'; MULTIPLY='*'; DIVIDE='/'; NEGATIVE='(-)';
        YEAR='YEAR'; MONTH='MONTH'; DAY='DAY';


    @classmethod
    def __formatval__(cls, val):
        if(hasattr(val, 'columnExpr')):
        #if(isinstance(val, F)):
            return val.columnExpr;
        #if(isinstance(val, int) or isinstance(val, np.int32) or isinstance(val, float) or isinstance(val, C)) :
        if(type(val) in AIDADtypes.numeric):
            return AIDADtypes.formatnumeric(val);
            #return '{}'.format(val);
        return val;

    def __str__(self):
        return 'F: {0}, {1}, {2}'.format(self._col1_, self._col2_, self._operator_)

    def __init__(self, col1, col2=None, operator=None):
        self._col1_ = col1;
        self._col2_ = col2;
        self._operator_ = operator;

    #Get a list of source columns referenced in this expression.
    @property
    def srcColList(self):
        scl = [];
        if(hasattr(self._col1_, 'srcColList')):
            scl += self._col1_.srcColList;
        if(hasattr(self._col2_, 'srcColList')):
            scl += self._col2_.srcColList;
        if(isinstance(self._col1_, str)):
            scl.append(self._col1_);
        return scl;

    @property
    def columnExprAlias(self):
        if(hasattr(self._col1_, 'columnExprAlias')):
            return  self._col1_.columnExprAlias;
        if(hasattr(self._col2_, 'columnExprAlias')):
            return  self._col2_.columnExprAlias;
        return self._col1_;

    #Form the SQL text for this column / expression
    @property
    def columnExpr(self):
        if(not self._operator_): #If this is not an expression, it is just the column.
            return self.__formatval__(self._col1_);
        if(self._operator_ in F.binaryOps): #If this is an expression with a binary operator, generate a combined expression.
            #logging.debug("{} {} {}".format(type(self._col1_), self._operator_.value, type(self._col2_)));
            return  '(' + F.__formatval__(self._col1_) + self._operator_.value + F.__formatval__(self._col2_) + ')';
        if(self._operator_== F.OP.NEGATIVE):  #Unary negative
            return '(' + '-'  + F.__formatval__(self._col1_) + ')';
        if(self._operator_ in F.extractOps):
            return 'EXTRACT(' + self._operator_.value +  ' FROM ' + F.__formatval__(self._col1_) + ')';



    # Methods to support linear algebra operations on columns.
    def __add__(self, other):
        return F(self, other, operator=F.OP.ADD);

    def __radd__(self, other):
        return F(other, self, operator=F.OP.ADD);

    def __sub__(self, other):
        return F(self, other, operator=F.OP.SUBTRACT);

    def __rsub__(self, other):
        return F(other, self, operator=F.OP.SUBTRACT);

    def __mul__(self, other):
        return F(self, other, operator=F.OP.MULTIPLY);

    def __rmul__(self, other):
        return F(other, self, operator=F.OP.MULTIPLY);

    def __truediv__(self, other):
        return F(self, other, operator=F.OP.DIVIDE);

    def __rtruediv__(self, other):
        return F(other, self, operator=F.OP.DIVIDE);

    def __neg__(self):
        return F(self, operator=F.OP.NEGATIVE);

#List of binart operators.
F.binaryOps  = (F.OP.ADD, F.OP.SUBTRACT, F.OP.MULTIPLY, F.OP.DIVIDE);
#List of functions to extract parts from a date
F.extractOps = (F.OP.YEAR, F.OP.MONTH, F.OP.DAY);

class EXTRACT(F):
    def __init__(self, col, extractoperator=None):
        super().__init__(col, None, extractoperator);


class SUBSTRING(F):
    def __init__(self, col, fromidx=1, len=None):
        super().__init__(col);
        self._fromidx_ = fromidx;
        self._len_ = len;

    #Form the SQL text for this column / expression
    @property
    def columnExpr(self):
        return 'SUBSTRING(' +  F.__formatval__(self._col1_) + ' FROM ' + str(self._fromidx_) + ( (' FOR '+ str(self._len_)) if(self._len_) else '' ) + ')';

class CASE(F):
    def __init__(self, cases, deflt=None):
        super().__init__(None, None);
        self._cases_ = cases;
        self._deflt_ = deflt;

    #Get a list of source columns referenced in this expression.
    @property
    def srcColList(self):
        scl = [];
        #Go through each conditions for case and extract source columns in it.
        for case_ in self._cases_:
            (cond, val) = case_;
            if(hasattr(cond, 'srcColList')): #For the condition check
                scl += cond.srcColList;
            if(hasattr(val, 'srcColList')):  #The value
                scl += val.srcColList;
            if(isinstance(val, str)):
                scl.append(val);

        if(self._deflt_ and hasattr(self._deflt_, 'srcColList') ):
            scl += self._deflt_.srcColList;

        return scl;

    @property
    def columnExprAlias(self):
        exprAlias=None;
        for case_ in self._cases_:
            (cond, val) = case_;
            if(hasattr(cond, 'columnExprAlias')):
                exprAlias = cond.columnExprAlias;
                if(exprAlias):
                    break;
        return ('case_' + exprAlias) if(exprAlias) else exprAlias;

    #Form the SQL text for this column / expression
    @property
    def columnExpr(self):
        expr='CASE ';
        for case_ in self._cases_:
            (cond, val) = case_;
            expr += ' WHEN ' + F.__formatval__(cond) + ' THEN ' + F.__formatval__(val);
        expr += ' ELSE ' + ( F.__formatval__(self._deflt_) if(self._deflt_) else 'NULL' ) + ' END ';
        return expr;



class TabularData(metaclass=ABCMeta):
    @abstractmethod
    def filter(self, *selcols): pass;

    @abstractmethod
    def join(self, otherTable, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER): pass;

    @abstractmethod
    def aggregate(self, projcols, groupcols=None): pass;

    @abstractmethod
    def project(self, projcols): pass;

    @abstractmethod
    def order(self, orderlist): pass;

    @abstractmethod
    def distinct(self): pass;

    @abstractmethod
    def loadData(self, matrix=False): pass;

    @abstractmethod
    def __add__(self, other): pass;

    @abstractmethod
    def __radd__(self, other): pass;

    @abstractmethod
    def __mul__(self, other): pass;

    @abstractmethod
    def __rmul__(self, other): pass;

    @abstractmethod
    def __sub__(self, other): pass;

    @abstractmethod
    def __rsub__(self, other): pass;

    @abstractmethod
    def __truediv__(self, other): pass;

    @abstractmethod
    def __rtruediv__(self, other): pass;

    @abstractmethod
    def __pow__(self, power, modulo=None): pass;

    @abstractmethod
    def __matmul__(self, other): pass;

    @abstractmethod
    def __rmatmul__(self, other): pass;

    @property
    @abstractmethod
    def T(self): pass;

    @abstractmethod
    def __getitem__(self, item): pass;

    #WARNING !! Permanently disabled  !
    #Weakref proxy invokes this function for some reason, which is forcing the TabularData objects to materialize.
    #@abstractmethod
    #def __len__(self): pass;

    @property
    @abstractmethod
    def shape(self): pass;

    @abstractmethod
    def vstack(self, othersrclist): pass;

    @abstractmethod
    def hstack(self, othersrclist, colprefixlist=None): pass;

    @abstractmethod
    def describe(self): pass;

    @abstractmethod
    def sum(self, collist=None): pass;

    @abstractmethod
    def avg(self, collist=None): pass;

    @abstractmethod
    def count(self, collist=None): pass;

    @abstractmethod
    def countd(self, collist=None): pass;

    @abstractmethod
    def countn(self, collist=None): pass;

    @abstractmethod
    def max(self, collist=None): pass;

    @abstractmethod
    def min(self, collist=None): pass;

    @abstractmethod
    def head(self,n=5): pass;

    @abstractmethod
    def tail(self,n=5): pass;

    @abstractmethod
    def _U(self, func, *args, **kwargs): pass;

    @abstractmethod
    def _genSQL_(self, *args, **kwargs): pass;

    @property
    @abstractmethod
    def cdata(self): pass;

#This is an object in the database.
class DBObject(metaclass=ABCMeta):
    pass;



#Base aggregation function.
class AggregateSQLFunction(metaclass=ABCMeta):
    def __init__(self, srcColName, distinct=False, funcName=None):
        self.__srcColName__  = srcColName;
        self.__distinct__    = distinct;
        self.__funcName__    = funcName;

    @property
    def funcName(self):
        return self.__funcName__;

    @property
    def sourceColumn(self):
        return self.__srcColName__;

    @property
    def genSQL(self):
        return self.__funcName__ + '(' +  ('DISTINCT ' if(self.__distinct__) else '') + self.__srcColName__ + ')';

    def __str__(self):
        return self.genSQL

#Specific types of aggregation functions.
class COUNT(AggregateSQLFunction):
    def __init__(self, srcColName, distinct=False):
        super().__init__(srcColName, distinct=distinct, funcName='COUNT');

    @property
    def genSQL(self):
        return self.__funcName__ + '(*)' if(self.__srcColName__ == '*') else super().genSQL;

class MAX(AggregateSQLFunction):
    def __init__(self, srcColName, distinct=False):
        super().__init__(srcColName, distinct=distinct, funcName='MAX');

class MIN(AggregateSQLFunction):
    def __init__(self, srcColName, distinct=False):
        super().__init__(srcColName, distinct=distinct, funcName='MIN');

class AVG(AggregateSQLFunction):
    def __init__(self, srcColName, distinct=False):
        super().__init__(srcColName, distinct=distinct, funcName='AVG');

class SUM(AggregateSQLFunction):
    def __init__(self, srcColName, distinct=False):
        super().__init__(srcColName, distinct=distinct, funcName='SUM');

