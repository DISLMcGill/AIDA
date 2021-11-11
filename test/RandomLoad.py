import random


class RandomLoad:
    def __init__(self, n=1, seed=1):
        self.__loaded = False
        self.__prob = 1 / n
        # chance of previous event not occur
        self.__cum_prob = 1
        random.seed(seed)

    def load_data_randomly(self, *args):
        print("prob = {}, cum_prob = {}, loaded = {}".format(self.__prob, self.__cum_prob, self.__loaded))
        tbs = list(args)
        if not self.__loaded:
            # the probability of current event and the previous events not occur
            cond_prob = self.__prob / self.__cum_prob
            r = random.random()
            # chance of cond_prob to load the data at this point
            if r < cond_prob:
                self.__loaded = True
                for i, table in enumerate(tbs):
                    print('args= {}, arg[i]={}'.format(args, args[i]))
                    table = table * 1
                    table.loadData()
                    print('loaded')
            # update the probability of current event not happening
            self.__cum_prob = (1 - cond_prob) * self.__cum_prob
        return tuple(tbs) if len(tbs) > 1 else tbs[0]
