import sys

from pathlib import Path
import json
import numpy as np
import pandas as pd

# bar data columns
BCOLS = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Trades', 'BidVolume','AskVolume']

def get_scid_df(filename, limitsize=sys.maxsize):
    f = Path(filename)
    # The file is opened and checked to exist. If the file does not exist, the program will raise an error "file not found".
    assert f.exists(), "file not found"
    stat = f.stat()
    # The size of the file is calculated and an offset value is determined, 56 or stat.st_size - ((limitsize // 40) * 40) whichever is smaller.
    # The offset value determines the position in the file from where the data should be read.
    # The purpose of using an offset is to skip a certain number of bytes from the beginning of the file.
    # In this code, the offset is calculated as follows:
    # offset = 56 if stat.st_size < limitsize else stat.st_size - ((limitsize // 40) * 40). 
    # If the file size (stat.st_size) is smaller than limitsize, the offset is set to 56. 
    # If the file size is larger than limitsize, the offset is calculated as stat.st_size - ((limitsize // 40) * 40),
    #  which means that only the last limitsize bytes of the file will be read. The division by 40 is likely to convert the number of bytes to
    # the number of records, as the dtype rectype has 9 fields, each field taking up a certain number of bytes.
    #  The result of limitsize // 40 gives the number of records that can fit in limitsize bytes, 
    # and multiplying this by 40 gives the number of bytes occupied by these records.
    #  Subtracting this from stat.st_size gives the offset from the beginning of the file where the data should be read.
    offset = 56 if stat.st_size < limitsize else stat.st_size - (
        (limitsize // 40) * 40)
    # A NumPy dtype is created to describe the structure of each record in the binary file.
    rectype = np.dtype([
        (BCOLS[0], '<u8'), (BCOLS[1], '<f4'), (BCOLS[2], '<f4'),
        (BCOLS[3], '<f4'), (BCOLS[4], '<f4'), (BCOLS[6], '<i4'),
        (BCOLS[5], '<i4'), (BCOLS[7], '<i4'), (BCOLS[8], '<i4')
    ])
    # The binary data is loaded into a NumPy memmap object, which allows for memory-mapped file access.
    df = pd.DataFrame(data=np.memmap(f, dtype=rectype, offset=offset, mode="r"), copy=False)
    # The DataFrame is cleaned to remove NaN values.
    df.dropna(inplace=True)
    # The time column is converted to datetime by subtracting 2209161600000000.
    df["Time"] = df["Time"] - 2209161600000000
    # Rows with a time outside the range of 1 to 1705466561000000 are dropped.
    df.drop(df[(df.Time < 1) | (df.Time > 1705466561000000)].index, inplace=True)
    # The time column is set as the index of the DataFrame and converted to a timezone-aware datetime.
    df.set_index("Time", inplace=True)
    df.index = pd.to_datetime(df.index, unit='us')
    df.index = df.index.tz_localize(tz="utc")
    df.index = df.index.tz_convert('America/Chicago')
    return df

df = get_scid_df('D:\\SierraChart\\SierraChart\\Data\\F.US.EPH23.scid', 100000000)
df['PriceAction'] = "Neutral"
print(df)


# You can then use pandas resample to get any timeframe. for example, for 1sec:
df_1sec = (
    df.resample("1S")
    .agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
            "Trades": "sum",
            "BidVolume": "sum",
            "AskVolume": "sum",
        }
    )
    .ffill()
)
print(df_1sec)

# Filter the DataFrame based on time and "BidVolume"
# don't process first 4 mins and last 11 mins of RTH
df_BV500 = df_1sec[
    (df_1sec.index.time >= pd.Timestamp("08:34").time()) & 
    (df_1sec.index.time <= pd.Timestamp("14:49").time()) & 
    (df_1sec["BidVolume"] > 450) &
    (df_1sec["AskVolume"] < 450)
]

print(df_BV500)


relevant_data_points = []

for i in range(len(df_BV500)):
    time = df_BV500.index[i]
    # get 5 seconds of data after the current time excluding the second in which BV was >500
    end_time = time + pd.Timedelta(seconds=5)
    filtered_rows_BV = df_1sec.loc[time:end_time][1:]
    filtered_rows_AV = df_1sec.loc[time:end_time]

    # combine all the rows into one
    merged_row_BV = pd.Series({
        "Time": filtered_rows_BV.index[0],
        "Open": filtered_rows_BV.iloc[0]["Open"],
        "High": filtered_rows_BV["High"].max(),
        "Low": filtered_rows_BV["Low"].min(),
        "Close": filtered_rows_BV.iloc[-1]["Close"],
        "Volume": filtered_rows_BV["Volume"].sum(),
        "Trades": filtered_rows_BV["Trades"].sum(),
        "BidVolume": filtered_rows_BV["BidVolume"].sum(),
        "AskVolume": filtered_rows_BV["AskVolume"].sum(),
    })

    merged_row_AV = pd.Series({
        "Time": filtered_rows_AV.index[0],
        "Open": filtered_rows_AV.iloc[0]["Open"],
        "High": filtered_rows_AV["High"].max(),
        "Low": filtered_rows_AV["Low"].min(),
        "Close": filtered_rows_AV.iloc[-1]["Close"],
        "Volume": filtered_rows_AV["Volume"].sum(),
        "Trades": filtered_rows_AV["Trades"].sum(),
        "BidVolume": filtered_rows_AV["BidVolume"].sum(),
        "AskVolume": filtered_rows_AV["AskVolume"].sum(),
    })



    # add the merged row to the merged_data DataFrame
    # with condition that BidVolume dies down and AskVolume starts coming in
    if merged_row_AV["AskVolume"] > 400 & merged_row_BV["BidVolume"] < 500:
        # extract datetime from Timestamp('2023-02-16 08:34:51-0600', tz='America/Chicago')
        date_time_only = str(time)[:-6]
        relevant_data_points.append(date_time_only)

# print the merged data
print(relevant_data_points)


# To print relevant_data_points list to a CSV file with each entry on a new line:
import csv
with open('output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    for item in relevant_data_points:
        writer.writerow([item])
















df_5sec = (
    df.resample("5S")
    .agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
            "Trades": "sum",
            "BidVolume": "sum",
            "AskVolume": "sum",
        }
    )
    .ffill()
)

#### AZURE anomaly detector code begin ####
import requests

ENDPOINT = "https://lg-test-8feb2023.cognitiveservices.azure.com/anomalydetector/v1.1-preview.1"
HEADERS = {
    "Ocp-Apim-Subscription-Key": "84d38f5a85654c9eb31afee50efe07e2"
}

API_MODEL = "{endpoint}/multivariate/models"
API_MODEL_STATUS = "{endpoint}/multivariate/models/{model_id}"
API_MODEL_INFERENCE = "{endpoint}/multivariate/models/{model_id}/detect"
API_MODEL_LAST_INFERENCE = "{endpoint}/multivariate/models/{model_id}/last/detect"
API_RESULTS = "{endpoint}/multivariate/results/{result_id}"
API_DELETE = "{endpoint}/multivariate/models/{model_id}"
SOURCE_BLOB_SAS = "[The SAS URL token that generated from your dataset in Azure Storage Account.]"

res = requests.get(API_MODEL.format(endpoint=ENDPOINT), headers=HEADERS)
assert res.status_code == 200, f"Error occured. Error message: {res.content}"
print(res.content)

#### AZURE anomaly detector code end ####

time_series_data = df_1sec.to_dict('records')

request_data = {"series": time_series_data, "granularity": "seconds"}
response = requests.post(endpoint, headers=headers, json=request_data)




##### Read the CSV file that has training data from OFL Bot: Begin #####
# Assuming that the CSV file time_intervals.csv has the following format:
# start_time,end_time,PriceAction
# 2023-02-02 08:00:00,2023-02-02 08:30:00,Bullish
# 2023-02-02 08:30:00,2023-02-02 09:00:00,Bearish
# 2023-02-02 09:00:00,2023-02-02 09:30:00,Bullish
df_time_intervals = pd.read_csv("time_intervals.csv")
start_time_list = df_time_intervals['start_time'].tolist()
end_time_list = df_time_intervals['end_time'].tolist()

# Create a list of tuples that represents the time intervals in the CSV
time_intervals = list(zip(start_time_list, end_time_list))

# Convert the time intervals in the dataframe to datetime
df_1sec.index = pd.to_datetime(df_1sec.index)

# Loop through the time intervals in the CSV
for interval in time_intervals:
    start, end = interval
    mask = (df_1sec.index >= start) & (df_1sec.index < end)
    df_1sec.loc[mask, 'PriceAction'] = df_time_intervals[(df_time_intervals['start_time'] == start) & 
                                                     (df_time_intervals['end_time'] == end)]['PriceAction'].values[0]

mask = (df.index >= "2023-02-02 08:44:06") & (df.index < "2023-02-02 08:48:55")
df.loc[mask].to_csv("output3.csv")

##### Read the CSV file that has training data from OFL Bot: End #####