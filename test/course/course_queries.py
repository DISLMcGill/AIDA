import random

from aida.aida import *;

config = __import__('courses-config')
seed = 101

def update_seed(sd):
    global seed
    seed = sd
    random.seed(seed)


class RandomLoad:
    def __init__(self, n=1):
        self.__loaded = False
        self.__prob = 1 / n
        # chance of previous event not occur
        self.__cum_prob = 1

    def load_data_randomly(self, *args):
        print("prob = {}, cum_prob = {}, loaded = {}".format(self.__prob, self.__cum_prob, self.__loaded))
        if not self.__loaded:
            # the probability of current event and the previous events not occur
            cond_prob = self.__prob / self.__cum_prob
            r = random.random()
            # chance of cond_prob to load the data at this point
            if r < cond_prob:
                self.__loaded = True
                for i, table in enumerate(args):
                    print('args= {}, arg[i]={}'.format(args, args[i]))
                    table = table * 1
                    args[i].loadData()
            # update the probability of current event not happening
            self.__cum_prob = (1 - cond_prob) * self.__cum_prob

def q01(dw):
    rl = RandomLoad(3)
    enrolls = dw.enroll
    rl.load_data_randomly(enrolls)
    se = enrolls.filter(Q('term', C('%2020%'), CMP.LIKE)).project(('sid'))
    rl.load_data_randomly(se)
    s = dw.student
    rl.load_data_randomly(s)
    s = s.filter(Q('sid', se, CMP.IN))
    return s


def q02(dw):
    rd = RandomLoad(4)
    offer = dw.courseoffer
    rd.load_data_randomly(offer)
    offer = offer.aggregate(('ccode', {COUNT('*'):'count'}), ('ccode', )).order(('count#desc',))
    offer = offer.filter(Q('count', C(8), CMP.GT))
    rd.load_data_randomly(offer)
    c = dw.course
    rd.load_data_randomly(offer, c)
    e = dw.enroll
    rd.load_data_randomly(e)
    j = c.join(offer, ('ccode', ), ('ccode',), COL.ALL, COL.ALL)
    j = j.join(e, ('ccode', ), ('ccode',), COL.ALL, COL.ALL)

    return j