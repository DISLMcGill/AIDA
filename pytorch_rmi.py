from aida.aida import *

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')

print('Register Service')
service = dw._MatrixFactorizationTorchRMI(1500, 1500, 3)
print('fit')
service.fit(dw.mf_data, (['user_id', 'movie_id'], 'rating'), 5000)