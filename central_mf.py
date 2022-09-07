from aida.aida import *
import numpy as np
import time

dw = AIDA.connect('whe_server_3', 'bixi', 'bixi', 'bixi', 'mf')

def run_mf(db, x, iterations, batch_size):
    import numpy as np
    users_info = list(x.project('user_id').distinct().order('user_id').cdata['user_id'])
    movies_info = list(x.project('movie_id').distinct().order('movie_id').cdata['movie_id'])
    users_matrix = dict.fromkeys(users_info, np.random.rand(3))
    movies_matrix = dict.fromkeys(movies_info, np.random.rand(3))
    weights = (users_matrix, movies_matrix)
    for i in range(iterations):
        batch = np.random.choice(x.shape[0], batch_size, replace=False)
        batch_x = x[batch, :].cdata
        users_update = {}
        movies_update = {}
        for p in range(batch_x['movie_id'].shape[0]):
            user = batch_x['user_id'][p]
            movie = batch_x['movie_id'][p]
            e = batch_x['rating'][p] - np.dot(weights[0][user], weights[1][movie])
            users_update[user] = users_update.get(user, 0) + 0.0002 * (2 * e * weights[1][movie] - 0.02 * weights[0][user])
            movies_update[movie] = movies_update.get(movie, 0) + 0.0002 * (2 * e * weights[0][user] - 0.02 * weights[1][movie])
        for u in users_update:
            weights[0][u] = weights[0][u] + users_update[u]
        for m in movies_update:
            weights[1][m] = weights[1][m] + movies_update[m]
    return weights

def score(x, weights):
    y = x.cdata
    e = 0
    for p in range(y['movie_id'].shape[0]):
        user = y['user_id'][p]
        movie = y['movie_id'][p]
        e = e + pow(y['rating'][p] - np.dot(weights[0][user], weights[1][movie]), 2)
        e = e + 0.01 * (np.sum(np.power(weights[0][user], 2)) + np.sum(np.power(weights[1][movie], 2)))
    return e

print("Starting non-distributed model...")
start = time.perf_counter()
weights = dw._X(run_mf, dw.mf_data, 10000, 25)
end = time.perf_counter()

print(f"Run time: {end-start}")
print(f"Score: {score(dw.mf_data, weights)}")



