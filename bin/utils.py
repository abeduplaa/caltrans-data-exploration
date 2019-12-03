import os

def _check_file(file, error=None):
    if error is None:
        error = "Error, file not found"
    
    if os.path.isfile(file) is False:
        raise OSError(error)


def locate_config(args):      
    if len(args) > 1:
        path = args[1]
        error = "user provided config.ini file path not found (e.g., python /path/to/config.ini)"
    else:
        path = os.path.join(os.getcwd(),'config.ini')
        error = "config.ini not found in project directory and not provided by user"    
        
    _check_file(path, error)
    
    return path