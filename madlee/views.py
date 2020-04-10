from django.conf import settings
from .misc.file import list_dir, join_path, is_dir
from .misc.dj import json_response



@json_response
def list_file(request):
    base = request.GET['base']
    base_folder = settings.base_folder[base]

    current = request.GET.get('current', '')
    filter = request.GET.get('filter', '')
    base_folder = join_path(base_folder, current)

    files = []
    folders = []

    names = list_dir(base_folder)
    for name in names:
        if is_dir(join_path(base_folder, name)):
            folders.append(name)
        else:
            files.append(name)


    return {
        'base': base,
        'current': current,
        'filter': filter,
        'files': files,
        'folders': folders
    }    
    
    
