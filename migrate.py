

class Transformer(object):
    TRANSFORM_KINDS = set(['update', 'insert'])

    def __init__(self):
        self.transforms = dict()

    def register(self, table, func):
        if table in self.transforms:
            print "*** ERROR: override transform for table {} ***".format(table)
        self.transforms[table] = func

    def transform(self, item):
        # only transform these calls
        if item['kind'] not in self.TRANSFORM_KINDS:
            return item
        action = self.transforms.get(item['table'])
        if action:
            action(item)
        return item


def value_for_key(item, key):
    """Raises ValueError if key is an unknown columnname"""
    index = item['columnnames'].index(key)
    return item['columnvalues'][index]
