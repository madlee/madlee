
########################################################################
### Folder and file operations
from os.path import split as split_path, join as join_path, normpath as normalize_path, abspath as abs_path
from os.path import isdir as is_dir, isfile as is_file, dirname
from os import makedirs as make_dirs, listdir as list_dir
from os import remove as remove_file
from os import access as access_file_permission
from os import W_OK, R_OK, X_OK, F_OK

def ensure_dirs(name):
    if not is_dir(name):
        make_dirs(name)


def split_filename(filename):
    folder, filename = split_path(filename)
    tokens = filename.split('.')
    filename = tokens[:-1]
    extname = tokens[-1]
    return folder, '.'.join(filename), extname


def writable_file(filename):
    if access_file_permission(filename, W_OK):
        return True
    else:
        return False


### Folder and file operation
########################################################################


