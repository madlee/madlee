
########################################################################
### Simple Algorithm
from random import random

def random_split(total, count, dec=None, min=0.5):
    mean = total/count
    result = [mean*(min + (1-min)*2*random()) for _ in range(count)]
    factor = total/sum(result)
    result = [i*factor for i in result]
    if dec is not None:
        result = [round(i, dec) for i in result]

    return result




def data_bin(data, factor, key=0, i=0, j=None, fillgap=False):
    if j == None:
        j = len(data)
    if i < j:
        ki, kj = data[i][key] // factor,  data[j-1][key] // factor
        if ki == kj:
            return [ data[i:j] ]
        else:
            result = []
            mid = (i+j) // 2

            result_l = data_bin(data, factor, key, i, mid)
            result_r = data_bin(data, factor, key, mid, j)
            if result_l and result_r:
                vl, vr = result_l[-1], result_r[0]
                if vl[0][key] // factor == vr[0][key] // factor:
                    result = result_l[:-1] + [vl+vr] + result_r[1:]
                else:
                    result = result_l + result_r
            else:
                result = result_l + result_r

        if fillgap and result:
            result2 = [result[0]]
            next = 1
            for next in range(1, len(result)):
                next_k = result[next][0][key] // factor
                result2 += [[]] * int(next_k - ki - 1)
                result2.append(result[next])
                ki = next_k
            result = result2

        return result
    else:
        return []


### Simple Algorithm
########################################################################

