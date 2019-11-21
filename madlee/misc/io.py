


########################################################################
### IO related

def print_bar(char='=', width=78):
    print (char*width)

def print_title(title, char='=', width=78):
    print_bar(char, width)
    for line in title.split('\n'):
        line = line.strip()
        n0 = width - len(line) - 6
        n1 = n0 // 2
        n2 = n0-n1
        print (char, ' '*n1, line, ' '*n2, char)
    print_bar(char, width)

def print_subtitle(title, char='-', width=78):
    for line in title.split('\n'):
        line = line.strip()
        n0 = width - len(line) - 2
        n1 = n0 // 2
        n2 = n0-n1
        print (' '*n1, line, ' '*n2)
    print_bar(char, width)


from pickle import loads as load_pickle, dumps as dump_pickle
from json import loads as load_json, dumps as dump_json


### IO related
########################################################################
