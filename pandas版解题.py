"""
作者：陶远红
日期：2024-03-27
名称：面试题
注意：该代码为测试环境，如果切到生产，需要根据业务需求进行更改
"""

import pandas as pd

data = [
    ('AE686(AE)', '7', 'AE686', 2022),
    ('AE686(AE)', '8', 'BH2740', 2021),
    ('AE686(AE)', '9', 'EG999', 2021),
    ('AE686(AE)', '10', 'AE0908', 2023),
    ('AE686(AE)', '11', 'QA402', 2022),
    ('AE686(AE)', '12', 'OA691', 2022),
    ('AE686(AE)', '12', 'OB691', 2022),
    ('AE686(AE)', '12', 'OC691', 2019),
    ('AE686(AE)', '12', 'OD691', 2017)
]

df = pd.DataFrame(data=data, columns=['peer_id', 'id_1', 'id_2', 'year'])
print(df)
print('----------------------------------分割线-------------------------------------')


# 要求一：如果peer_id 包含了 id_2，则取年份，比如第0行,peer_id包含了id_2的值，则取2022
def koo(df):
    return df['id_2'] in df['peer_id']


new_df = df[df.apply(koo, axis=1)][['peer_id', 'year']]
print(new_df)
print('----------------------------------分割线-------------------------------------')
number = eval(input('请输入数字'))


# 需求二：基于需求一获取到的年份,提取到小于等于需求一当中的年份数据
def step_two(new_df, number):
    years = new_df.year.values[0]
    df2 = df[df['year'] <= years]
    # 依据年份进行分组聚合
    df3 = df2.groupby(['year'])['peer_id'].count().reset_index().sort_values('year', ascending=False)
    # 通过数字number来确定要多少条数据
    number = number
    i = 1
    while True:
        df4 = df3.head(i)
        result = sum(df4.peer_id.values.tolist())
        if result >= number or i > 10:
            break
        else:
            i += 1
            continue
    return df4


def step_three(df4):
    set_year = df4.year.values.tolist()
    t1 = df.query('year in {}'.format(set_year))[['peer_id', 'year']].drop_duplicates()
    list_index = t1.index.tolist()
    list_data = []
    for i in list_index:
        list_data.append(tuple(t1.loc[i]))
    return list_data


df4 = step_two(new_df, number)
list_data = step_three(df4)

print(list_data)
