import pandas as pd

df=pd.read_csv('/home/ncamiso.khanyile/Data/data_store_rawvalue/raw.csv')

print('before sampling')
print(df.shape)
df=df.sample(frac=.1, random_state=200)
print('after sampling')
print(df.shape)
df.to_csv('/home/ncamiso.khanyile/Data/data_store_rawvalue/raw_sampled.csv')