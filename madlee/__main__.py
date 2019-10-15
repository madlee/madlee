#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from os.path import join as join_path, abspath as abs_path


def main():
    base_folder = abs_path(join_path(__file__, '../..'))
    sys.path.append(base_folder)

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'madlee.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
