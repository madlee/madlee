from django.conf import settings

from .misc.file import list_dir, join_path, is_dir
from .misc.file import last_modified_at, get_file_size
from .misc.dj import json_response

@json_response
def list_file(request):
    path = request.GET.get('path', '')
    archive_file_as_folder = request.GET.get('archive_file_as_folder', False)
    if path:
        path_list = path.split('/', 1)
        base_folder = path_list[0]
        folder_func, filter_func = settings.MADLEE_FOLDERS_FOR_BROWSE[base_folder]
        if type(folder_func) == str:
            real_folder = folder_func.format(request.user.username)
        else:
            real_folder = folder_func(base_folder, request)

        if len(path_list) == 1:
            real_path = real_folder
            file_path = ''
        else:
            path_list = path_list[1].split('#')
            real_path = join_path(real_folder, path_list[0])
            if len(path_list) > 1:
                file_path = real_path
                in_file = path_list[1]                
            else:
                in_file = ''
                if is_dir(real_path):
                    file_path = ''
                else:
                    file_path = real_path

        if file_path:
            # TODO: List File in archive files.
            pass
        else:
            files = []
            folders = []
            for name in list_dir(real_path):
                if filter_func(request, path, real_path, name):
                    full_path = join_path(real_path, name)
                    if is_dir(full_path):
                        folders.append({'name': name, 'time': last_modified_at(full_path)})
                    else:
                        files.append({'name': name, 'time': last_modified_at(full_path), 'size': get_file_size(full_path)})
    else:
        files = []
        folders = [{
            'name': k,
            'time': last_modified_at(
                folder_func.format(request.user.username) if type(folder_func) == str else folder_func(k, request.user) 
            )
        } for k, (folder_func, _) in settings.MADLEE_FOLDERS_FOR_BROWSE.items()
        ]

    return {
        'path': path,
        'folders': folders,
        'files': files,
        'folders': folders
    }    
