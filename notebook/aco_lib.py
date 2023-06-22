import requests
import os

def download_file(url, path=''):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()

        if path == '':
            pass
        else:
            os.makedirs(path, exist_ok=True) 
            path = path + '/'
            
        with open(path + local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=4000): 
                f.write(chunk)
    return local_filename