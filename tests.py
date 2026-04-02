import pandas
import pandas as pd
import torch
import matplotlib.pyplot as plt


def to_elapsed_seconds(col):
    """Convert a UTC timestamp column to elapsed seconds from the first entry."""
    parsed = pd.to_datetime(col, format='ISO8601')
    return (parsed - parsed.iloc[0]).dt.total_seconds()


def combine_by_second(df):
    """
    Combine rows with the same elapsed_seconds by summing quantity,
    and taking the last value for cumulative columns.
    """
    return df.groupby('time').agg(
        quantity=('quantity', 'sum'),
        cumulative_bought_qty=('cumulative_bought_qty', 'last'),
        cumulative_sold_qty=('cumulative_sold_qty', 'last'),
        probability_percent=('probability_percent', 'min'),
    ).reset_index()


df = pd.read_csv('data/KXWTAMATCH-26APR01STAGOR-STA.csv')

df['time'] = to_elapsed_seconds(df['received_time_utc'])

df['bought'] = df['cumulative_bought_qty'] - df['cumulative_sold_qty']

print(df.columns)

print(df['time'].duplicated().sum())


print(df[['time', 'probability_percent']])

df_combine = combine_by_second(df)

print(df_combine.shape)


fig, ax1 = plt.subplots()

ax1.plot(df['time'], df['probability_percent'], color='blue', label='Probability')
ax1.tick_params(axis='y', labelcolor='blue')

ax2 = ax1.twinx()
ax2.plot(df['time'], df['bought'], color='red', label='Bought')

plt.title('Probability for Pegula')
plt.show()


plt.plot(df_combine['time'], df_combine['probability_percent'])
plt.show()

