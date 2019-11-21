






########################################################################
###  Database Related

def execute_sqls(sql, cursor):
    '''Execute multi SQL seperated by ';' '''
    for i in sql.split(';'):
        i = i.strip()
        if i:
            cursor.execute(i)

###  Database Related
########################################################################

