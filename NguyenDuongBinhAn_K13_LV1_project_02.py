import csv
import os.path
import os
import json
import requests
import pandas as pd

def collect_product_data(api_url, product_ids, batch_size = 1000):
    total_products = len(product_ids)
    for i in range(0, total_products, batch_size):
        batch_num = i//batch_size
        output_path = os.path.curdir + f'/products_data/products_batch_{batch_num}.json'
        if os.path.exists(output_path):
            continue
        print(f'Processing batch {batch_num}')
        batch_ids = product_ids[i:i+batch_size]
        batch_products = []
        err_ids = []
        for id in batch_ids:
            try:

                headers = {'User-Agent' : 'Mozilla/5.0'}
                response = requests.get(f'{api_url}/{id}', headers = headers)
                product_data = response.json()
                filter_product_data = {
                    "id" : product_data.get("id"),
                    "name": product_data.get("name"),
                    "url_key": product_data.get("url_key"),
                    "price": product_data.get("price"),
                    "description": product_data.get("description"),
                    "image_url": product_data.get("images")[0].get("base_url")
                }
                batch_products.append(filter_product_data)
            except Exception as e:
                err_ids.append(id)
        output_path = os.path.curdir + f'/products_data/products_batch_{batch_num}.json'
        with open(output_path, 'w', encoding = 'utf-8') as output_file:
            json.dump(batch_products, output_file, ensure_ascii= False, indent = 4)

        if err_ids != []:
            output_path = os.path.curdir + f'/error_products/products_batch_{batch_num}.csv'
            with open(output_path, 'w', encoding = 'utf-8') as output_file:
                output_file.write("id\n")
                for err_id in err_ids:
                    output_file.write(f'{err_id}\n')

product_ids = pd.read_csv('products-0-200000.csv', usecols=[0]).squeeze().values
collect_product_data(api_url='https://api.tiki.vn/product-detail/api/v1/products', product_ids= product_ids)



