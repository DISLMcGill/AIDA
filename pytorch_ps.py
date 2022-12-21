from aida.aida import *

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')
n_users = dw.mf_data.project('user_id').distinct().count()
n_movies = dw.mf_data.project('movie_id').distinct().count()

print('Register Service')
service = dw._MatrixFactorizationPSModel(n_users, n_movies, 3)
print('fit')
service.fit(dw.mf_data, (['user_id', 'movie_id'], ['rating']), 5000)