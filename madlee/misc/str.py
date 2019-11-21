
########################################################################
###  String related

from re import compile as compile_re
DIGITS = compile_re(r'\d+')
def all_digits(v):
    result = DIGITS.findall(v)
    return ''.join(result)


TRUE_VALUES = {'t', 'true', 'y', 'yes', '1'}
FALSE_VALUES = {'f', 'false', 'n', 'no', '0'}
def to_bool(v):
    v = v.lower()
    if v in FALSE_VALUES:
        return False
    elif v in TRUE_VALUES:
        return True
    else:
        raise ValueError(v)

###  String related
########################################################################


