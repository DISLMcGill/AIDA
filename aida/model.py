from aidacommon.dborm import *

class Model(ABCMeta):
    def __init__(self, dw):
        self.dw = dw
        self.model_service = dw._RegisterModel(self)

    def fit(self, x, iterations=10000, batch_size=25):
        self.model_service.fit(x, iterations, batch_size)

    @abstractmethod
    def get_params(self): pass;

    @abstractmethod
    def initialize(self, x): pass;

    @abstractmethod
    def aggregate(self, results): pass;

    @staticmethod
    @abstractmethod
    def score(db, x, weights): pass;

    @staticmethod
    @abstractmethod
    def preprocess(db, x): pass;

    @staticmethod
    @abstractmethod
    def iterate(db, x, weights, batch_size): pass;

