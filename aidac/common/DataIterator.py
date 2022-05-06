def generator(data):
    rs = []
    for row in data.to_numpy():
        rs.append(tuple(row))
    return rs

