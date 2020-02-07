
import pandas as pd


df = pd.DataFrame(
    data = {'Values': range(1,100000)}
)


from datetime import datetime
now1 = datetime.now()

summed = 0
for  idx, row in df.iterrows():
    summed += row['Values']

now2 = datetime.now()
print(now2 - now1)

summed



from datetime import datetime
now1 = datetime.now()

summed = 0

for row in df.itertuples(index=True):
    print(row.Index)
    summed += row.Values

now2 = datetime.now()
print(now2 - now1)

summed