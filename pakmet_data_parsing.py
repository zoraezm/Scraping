import os
import pandas as pd
from datetime import datetime, timedelta

lst_lst_df = []
for filename in os.listdir("input/"):
    if filename != '.DS_Store':
        print(filename)
        df = pd.read_csv('input/'+filename)
        # print(df.head())
        df_un_dates = df.drop_duplicates(subset='datetime')
        lst_df = []
        for date in df_un_dates['datetime']: # 00Z01JUN2021
            hours = 5 # 5 hours offset from Zulu to get GMT-5 for Pakistan
            hours_added = timedelta(hours=hours)
            date_time_obj = datetime.strptime(date,'%HZ%d%b%Y')+hours_added
            df_subset = df[df['datetime'] == date]
            df_subset = df_subset.drop(columns=['datetime'])
            df_subset = df_subset.set_index(['lat'])
            df_subset = df_subset.stack().reset_index()
            df_subset.rename(columns={'level_1': 'lon', 0: filename.replace('.csv','')}, inplace=True)
            df_subset.index.name = 'id'
            df_subset['datetime_str'] = date
            df_subset['datetime'] = date_time_obj
            lst_df.append(df_subset)
            # file_name = 'output/'+filename.replace('.csv','')+'_'+date+'.csv'
            # df_subset.to_csv(file_name)
            # print(df_subset.head())
        lst_lst_df.append(lst_df)

var_dfs = []
for ls in lst_lst_df:
    df = pd.concat(ls)
    var_dfs.append(df)
    # print('done')

print(len(var_dfs))
df_final = pd.concat(var_dfs, axis=1)
# df_final.drop_duplicates(subset=['lat','lon','datetime'],keep='first')
df_final = df_final.loc[:,~df_final.columns.duplicated()]
# df_final = df_final[df_final['datetime'] == '']
df_final.to_csv('ouput4/weather.csv')
print('done')