

TRANSFORMS = dict()


def register_transform(table, func):
    if table in TRANSFORMS:
        print "*** ERROR: override transform for table {} ***".format(table)
    TRANSFORMS[table] = func


def transform(item):
    # only transform these calls
    if item['kind'] not in set(['update', 'insert']):
        return item
    action = TRANSFORMS.get(item['table'])
    if action:
        action(item)
    return item
