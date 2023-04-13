from aida.aida import *
from sklearn.datasets import make_blobs
from sklearn.cluster import DBSCAN
from sklearn.cluster import KMeans
import numpy as np

class FirstStep:
    @staticmethod
    def worker_clustering(dw, data, context=None):
        from sklearn.cluster import DBSCAN
        from sklearn.cluster import KMeans
        import numpy as np
        import logging

        logging.info('Worker started clustering')

        db = DBSCAN(eps=1.0 / 2, min_samples=6).fit(data.matrix.T)
        classes = []
        for i in range(2):
            classes.append(data[np.where(db.labels_ == i)])
        clusts = [KMeans(n_clusters=3).fit(c) for c in classes]
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
    def global_clustering(dw, results):
        from sklearn.cluster import DBSCAN
        import logging

        logging.info('Middleware started global fix')

        centers = sum(results, [])
        epsilon = max([r[1] for r in centers])

        db = DBSCAN(eps=epsilon, min_samples=2).fit([r[0] for r in centers])

        return db

    def work(self):
        return FirstStep.worker_clustering

    def aggregate(self):
        return FirstStep.global_clustering

class SecondStep:
    @staticmethod
    def fix_local_clusters(dw, data, context=None):
        from scipy import spatial
        import logging

        logging.info('Worker started cluster adjustment')

        db_results = context['previous']
        tree = spatial.cKDTree(data)
        labels = np.asarray([-1] * len(data))
        eps = db_results.eps
        centers = zip(db_results.components_, db_results.labels_)
        for point, label in centers:
            cluster = tree.query_ball_point(point, eps)
            labels[cluster] = label
        data.hstack(labels)
        return

    @staticmethod
    def final_agg(dw, results):
        return

    def work(self):
        return SecondStep.fix_local_clusters

    def aggregate(self):
        return SecondStep.aggregate


dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'wa')
print('starting work!')
dw._workAggregateJob([FirstStep(), SecondStep(), dw.cluster_data])
print('finished work!')
print(dw.cluster_data.cdata)