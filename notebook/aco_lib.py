import requests
import os
import datetime
from dateutil.relativedelta import relativedelta

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


def get_last_months(months, year=datetime.date.today().strftime("%Y"), month=datetime.date.today().strftime("%m")):
    start_date = datetime.datetime.strptime(f'{year}-{month}', '%Y-%m')

    result = []
    for i in range(months):
        result.append((start_date.year,start_date.month))
        start_date += relativedelta(months = -1)

    return result