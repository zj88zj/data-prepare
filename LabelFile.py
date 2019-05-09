import pandas as pd
import simplejson

tax = simplejson.load(open("../../Work/merged-taxonomy/markable_merged_taxonomy.json", "rb"))

for (key, value) in tax.items():
    drop = {i:tax[i] for i in tax if i!=key}
    x = []
    for (k, v) in drop.items():
        for item in v["path"].split(","):
            x.append(item)
    if any(key in s for s in x):
        value["isleaf"] = False
    else: value["isleaf"] = True

leafhashes = []
for (key, value) in tax.items():
    if value["isleaf"] == True:
        leafhashes.append(key)

with open('./leafhashes.txt', 'w') as f:
    for item in leafhashes:
        f.write("%s\n" % item)

original_df = pd.read_csv("../../Work/merged-taxonomy/wayfair_furniture.csv")
valid_df = original_df[original_df.path.apply(lambda x: x.split(',')[-2] in leafhashes)].copy()
valid_df = valid_df.drop_duplicates(subset="image_hash", keep=False)
valid_df = valid_df.reset_index(drop=True)

leaf_to_cate = {}
for (key, value) in tax.items():
    if key in leafhashes:
        leaf_to_cate[key] = tax[value['path'].split(',')[3]]['name']

valid_df['category'] = valid_df.path.apply(lambda x: x.split(',')[-2]).map(leaf_to_cate)

valid_df.to_csv("./wayfair_taxonomy_train.csv", index = False)
