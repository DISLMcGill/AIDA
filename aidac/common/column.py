class Column:
    def __init__(self, name, dtype, table=None, schema=None, nullable=True, srccol=None, transform=None):
        self.name = name
        self.dtype = dtype
        self.tablename = table
        self.schema = schema
        self.nullable = nullable
        self.srccol = srccol
        self.transform = transform