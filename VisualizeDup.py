import matplotlib.pyplot as plt
import pandas as pd 
import numpy as np
import cv2
import os
from termcolor import colored
from sklearn.metrics import confusion_matrix
import seaborn as sns

df = pd.read_csv("./duplicates.csv")
uniq_hash = pd.DataFrame(df["image_hash"].unique())
uniq_hash.columns = ["image_hash"]

# #------- based on product --------#
# uniq_hash["is_diff"]=uniq_hash.image_hash.map(df.groupby("image_hash").product_hash.nunique() > 1)
# diff_df =  uniq_hash.loc[uniq_hash["is_diff"]==True]
# diff_df = diff_df.reset_index(drop=True)
# same_df =  uniq_hash.loc[uniq_hash["is_diff"]==False] #there are 5 duplicated images duplicated within same products

# #show the i th image in diff_df
# print("--from 0 to 99--")
# num = input("Enter the index of image:")
# i = int(num) 
# img_dir = "./wayfair_image_100"
# img_path = os.path.join(img_dir, "{}.jpg".format(diff_df["image_hash"][i]))
# img = cv2.imread(img_path)
# if img is not None:
#     cv2.imshow(img_path, img)
#     cv2.waitKey(2)
#     cv2.destroyAllWindows()
# else:
#     print(colored("Error Loading:" + img_path, "red"))

# #products this image related to
# #show images according to product 
# orig_df = pd.read_csv("../../Work/merged-taxonomy/wayfair_furniture.csv")
# for item in df.loc[df["image_hash"]==diff_df["image_hash"][i]].product_hash.head(5):
#     fig = plt.figure()
#     img_array = []
#     for path in orig_df.loc[orig_df["product_hash"]==item].image_hash.head(5):
#         img = cv2.imread(os.path.join(img_dir, "{}.jpg".format(path)))
#         if img is not None:
#             dim = (100, int(img.shape[0]*(100.0/img.shape[1])))
#             img_array.append(cv2.resize(img, dim, interpolation=cv2.INTER_AREA))
#         else: print(colored("Error Loading:" + path,"red"))
#     for x in range(len(img_array)):
#         ax = fig.add_subplot(5, 5, x + 1)
#         ax.imshow(img_array[x], interpolation='nearest')
#         plt.axis('off')
#     fig.suptitle('product#{}'.format(item))
#     print(colored('product#{}'.format(item), "green"))
#     print("image_hash:"+ orig_df.loc[orig_df["product_hash"]==item].image_hash.head(5))
# plt.show()

          
# #Select images from original folder based on product_hash (on server :73)
# prod_hash = pd.DataFrame(columns = ["product_hash"])
# for item in  diff_df["image_hash"][:100]:  
#     hash_dic = pd.DataFrame(columns = ["product_hash"]) 
#     hash_dic.product_hash = orig_df.loc[orig_df.image_hash == item].product_hash   
#     prod_hash =pd.concat([prod_hash,hash_dic])       
# prod_hash = prod_hash.product_hash.unique()
# output = orig_df[orig_df.product_hash.apply(lambda x: x in prod_hash)]
# image_hash_list = output.image_hash.values.tolist()
# image_hash_list = [s + ".jpg" for s in image_hash_list]
# with open('./image_hash_list.txt', 'w') as outputfile:  
#     for line in image_hash_list:
#         outputfile.write('%s\n' % line)
# #termianl:
    # #rsync -a ./furniture_images/ --files-from=/root/data-preparation/scraper/wayfair/image_hash_list.txt ./wayfair_image_100/


#------- based on category ------#
uniq_hash["is_diff"]=uniq_hash.image_hash.map(df.groupby("image_hash").category.nunique() > 1)
diff_df =  uniq_hash.loc[uniq_hash["is_diff"]==True]
diff_df = diff_df.reset_index(drop=True)

#show the i th image in diff_df
# print("--from 0 to 433--")
# num = input("Enter the index of image:")
# i = int(num) 
# img_dir = "./wayfair_image_400/"
# img_path = os.path.join(img_dir, "{}.jpg".format(diff_df["image_hash"][i]))
# img = cv2.imread(img_path)
# if img is not None:
#     cv2.imshow(img_path, img)
#     cv2.waitKey(600)
#     cv2.destroyAllWindows()
# else:
#     print(colored("Error Loading:" + img_path, "red"))
# print(df.loc[df["image_hash"]==diff_df["image_hash"][i]].category.unique())

# #Select images from original folder based on product_hash (on server :73)
# image_hash_list = diff_df["image_hash"].tolist()
# image_hash_list = [s + ".jpg" for s in image_hash_list]
# with open ('./image_list.txt', 'w') as outfile:
#     for line in image_hash_list:
#         outfile.write('%s\n' % line)
# #termianl:
#     #rsync -a ./furniture_images/ --files-from=/root/data-preparation/scraper/wayfair/image_list.txt ./wayfair_image_400/

#confusion matrix
category_all = []
category_list = []
x = []
y = []
for i in range(diff_df.index[-1]):
    category_all = df.loc[df["image_hash"]==diff_df["image_hash"][i]].category.unique().tolist()
    category_list += category_all
    if len(category_all) == 2 :
        x.append(category_all[0])
        y.append(category_all[1])

category_uniq = []
for item in category_list:
    if item not in category_uniq:
        category_uniq.append(item)

cm = confusion_matrix(x, y, category_uniq)
df_cm = pd.DataFrame(
        cm, index=category_uniq, columns=category_uniq, 
    )
heatmap = sns.heatmap(df_cm, annot=True, fmt="d")
plt.show()