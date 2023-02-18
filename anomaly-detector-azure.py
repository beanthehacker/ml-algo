



# Define the data and the parameters
data = [{"timestamp": row[0], "value": row[1]} for row in csv_data]
params = {
    "granularity": "seconds",
    "sensitivity": "high"
}

# Define the headers
headers = {
    'Content-Type': 'application/json',
    'Ocp-Apim-Subscription-Key': subscription_key
}

# Make the request
response = requests.post(endpoint, headers=headers, json={"series": data, "parameters": params})

# Get the response
if response.status_code == 200:
    result = response.json()
    # Check for the anomalies
    for i in range(len(result["isAnomaly"])):
        if result["isAnomaly"][i]:
            print("Anomaly detected at", csv_data[i][0])
        # else:
        #     print("No anomaly detected at", csv_data[i][0])
else:
    print("Request failed. Response code:", response.status_code)
