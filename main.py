import requests
import argparse
from multiprocessing import Pool
import pandas as pd
import urllib
from tqdm import *

def getCountry(loc):
    location = str(loc)
    if location.lower() == 'nan':
        return 'No Results'
    elif location.lower() == 'us':
        return 'United States'
    elif location.lower() == 'cn':
        return 'China'
    else:
        query = { 'location': location }
        url_encoded_loc = urllib.parse.urlencode(query)
        url = f'https://api.williamlin.tech/geocoder/country?{url_encoded_loc}'
        country = requests.get(url).text
        return country if not country.isdigit() else 'No Results'

def main():
    argparser = argparse.ArgumentParser(description='Country Retrieval Tool.')
    argparser.add_argument(
        '--file',
        '-f',
        type=str,
        default='data/sample.csv', help='File with location data.'
    )
    argparser.add_argument(
        '--out',
        '-o',
        type=str,
        default='out.csv', help='Output file.'
    )
    argparser.add_argument(
        '--pool',
        '-p',
        type=int,
        default=10, help='Number of processes to use.'
    )
    args = argparser.parse_args()

    data = None
    file = args.file
    if file.endswith('.csv'):
        data = pd.read_csv(file)
    else:
        print('File must be a JSON or CSV file.')
        sys.exit(0)
    
    with Pool(processes=args.pool) as p:
        test_data = data['actor_attributes_location']
        max_t = len(test_data)
        data_out = []
        with tqdm(total=max_t) as pbar:
            for i, res in tqdm(enumerate(p.imap(getCountry, test_data))):
                pbar.update()
                data_out.append(res)
        df_res = pd.DataFrame(list(zip(test_data, data_out)), columns=['Location', 'Country'])
        print(df_res)
        df_res.to_csv(args.out, index=False)

if __name__ == '__main__':
    main()
