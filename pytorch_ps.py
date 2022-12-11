from aida.aida import *
import torch
from torch.utils.data import Dataset

class MatrixFactorization(torch.nn.Module):
    def __init__(self, n_users, n_items, n_factors=3):
        super().__init__()
        self.user_factors = torch.nn.Embedding(n_users, n_factors, sparse=True)
        self.item_factors = torch.nn.Embedding(n_items, n_factors, sparse=True)

    def forward(self, data):
        return (self.user_factors(data[0]) * self.item_factors(data[1])).sum(1)

def preprocess(x):
    from torch.utils.data import Dataset
    class CustomDataset(Dataset):
        def __init__(self, data):
            self.data = data.cdata

        def __len__(self):
            return len(self.data['user_id'])

        def __getitem__(self, idx):
            return (torch.LongTensor(self.data['user_id'][idx]), torch.LongTensor(self.data['movie_id'][idx])), torch.FloatTensor(
                self.data['rating_id'][idx])

    return CustomDataset(x)

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')
n_users = dw.mf_data.project('user_id').distinct().count()
n_movies = dw.mf_data.project('movie_id').distinct().count()

print('Register Service')
service = dw._MatrixFactorizationPSModel(n_users, n_movies, 3)
print('fit')
service.fit([dw.mf_data], preprocess, 5000)