

class Transformer(object):
    TRANSFORM_KINDS = set(['update', 'insert'])

    def __init__(self):
        self.transforms = dict()

    def register(self, table, func):
        """
        Register a function to handle all inserts/updates on a table.
        func takes an item (logical repl. info) and returns either a modified item
        or a list/tuple if one update should convert to multiple updates
        TODO: also deletes

        """
        if table in self.transforms:
            print "*** ERROR: override transform for table {} ***".format(table)
        self.transforms[table] = func

    def transform(self, item):
        # only transform these calls
        if item['kind'] not in self.TRANSFORM_KINDS:
            return item
        action = self.transforms.get(item['table'])
        if action:
            return action(item)
        else:
            return item


def value_for_key(item, key):
    """Raises ValueError if key is an unknown columnname"""
    index = item['columnnames'].index(key)
    return item['columnvalues'][index]
