import pandas as pd

datas = pd.read_csv("../data/TedAwardNotices/csv/2017",dtype=str)
datas = datas[datas["CAE_NATIONALID"]=="2017350968"]
print(datas)