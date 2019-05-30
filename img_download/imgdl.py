import simplejson
import json
import hashlib
import argparse
import os
import pandas as pd
import dask.dataframe as dd
from dask.multiprocessing import get

parser = argparse.ArgumentParser(description='Image Downloader')
parser.add_argument('--products_info_json', type=str,
                    help='path to the json file which stores product information')
parser.add_argument('--products_tgt_csv', type=str,
                    help='path to the target csv file which will store flat information')
parser.add_argument('--image_fd', type=str,
                    help='path to the folder which will store downloaded images')
parser.add_argument('--max_workers', default=10, type=int,
                    help='concurrency when downloading images')

args = parser.parse_args()

# extract imagelinks
df = pd.read_csv(args.products_info_json)
def parse(df):
    return df.images.apply(lambda x: [{'image_url': eachimg['uri'], 'category': eachbox['category'], **eachbox['bounding_box']} for eachimg in json.loads(x) if 'detections' in eachimg and len(eachimg['detections']) for eachbox in eachimg['detections']])
ddf = dd.from_pandas(df, npartitions=40)
info = ddf.map_partitions(parse, meta=list).compute(get=get)
info_list = [l for item in info.tolist() for l in item] 
products_df = pd.DataFrame(info_list)
# with open(args.products_info_json, 'r') as f:
#         for cnt, line in enumerate(f):
#                 if line == '{\n' or line == '}': 
#                          continue
#                 line = line.strip('\n').strip(',')
#                 items = simplejson.loads('{'+line+'}') 
#                 products.update(items)
# ntot = sum([len(product_data['info']['images']) for _, product_data in products.items()])

# products_data = [[]] * ntot
# i = 0
# columns = None
# for product_hash, product_data in products.items():
#     if columns is None:
#         columns = [k for k in product_data if not k == 'info'] + \
#                 [k for k in product_data['info'] if not k == 'images'] + \
#                 ['product_hash', 'image_url']
#     fixed_ = [v for k,v in product_data.items() if not k == 'info'] + \
#             [v for k,v in product_data['info'].items() if not k == 'images'] + \
#             [product_hash]
#     product_data_ = [fixed_ + [image_url] for image_url in product_data['info']['images']]
#     products_data[i:i+len(product_data_)] = product_data_
#     i += len(product_data_)

products_df['image_hash'] = products_df['image_url'].apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())
if not os.path.exists(args.image_fd):
    os.mkdir(args.image_fd)

from downloader import Downloader
downloader = Downloader()
downloader.download_images(products_df.image_url.values, args.image_fd, max_workers=args.max_workers)

image_hashes = list(map(lambda x: x.split('.')[0], os.listdir(args.image_fd)))
products_df = products_df[products_df.image_hash.isin(image_hashes)].copy()
products_df.to_csv(args.products_tgt_csv, index=False)


