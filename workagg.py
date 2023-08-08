from aida.aida import *
from sklearn.datasets import make_blobs
from sklearn.cluster import DBSCAN
from sklearn.cluster import KMeans
import numpy as np

class FirstStep:
    @staticmethod
    def work(dw, data, context=None):
        from sklearn.cluster import DBSCAN
        from sklearn.cluster import KMeans
        import numpy as np
        import logging

        logging.info('Worker started clustering')

        dw.matrix_data = data.matrix.T
        db = DBSCAN(eps=1.0 / 2, min_samples=6).fit(dw.matrix_data)
        classes = []
        for i in range(len(np.unique(db.labels_))):
            classes.append(dw.matrix_data[np.where(db.labels_ == i)])
        clusts = [KMeans(n_clusters=9).fit(c) for c in classes]
        rep = []
        for j in range(len(clusts)):
            centers = clusts[j].cluster_centers_
            dists = []
            for i in range(len(centers)):
                sub_clust = classes[j][np.where(clusts[j].labels_ == i)]
                dist = max([np.linalg.norm(centers[i] - point) for point in sub_clust])
                dists.append(dist)
            rep += zip(centers, dists)
        return rep

    @staticmethod
    def aggregate(dw, results, context):
        from sklearn.cluster import DBSCAN
        import logging

        logging.info('Middleware started global fix')

        centers = sum(results, [])
        epsilon = max([r[1] for r in centers])

        db = DBSCAN(eps=epsilon, min_samples=2).fit([r[0] for r in centers])

        return db


class SecondStep:
    @staticmethod
    def work(dw, data, context=None):
        from scipy import spatial
        import numpy as np
        import logging

        logging.info('Worker started cluster adjustment')

        db_results = context['previous']
        tree = spatial.cKDTree(dw.matrix_data)
        labels = np.asarray([-1] * len(dw.matrix_data))
        eps = db_results.eps
        centers = zip(db_results.components_, db_results.labels_)
        for point, label in centers:
            cluster = tree.query_ball_point(point, eps)
            labels[cluster] = label
        return data.hstack(labels)

    @staticmethod
    def aggregate(dw, results, context):
        dw.results = dw._LoadDistTabularData(results)


dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'wa')
print('starting work!')
dw._workAggregateJob([FirstStep(), SecondStep()], dw.cluster_data)
print('finished work!')
print(dw.results.cdata)