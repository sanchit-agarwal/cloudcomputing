import json
import pandas as pd
import random

def lambda_handler(event, context):

    data = pd.read_json(event["data"])
    shots = int(event["shots"])
    price_history = int(event["price_history"])

    output_df = pd.DataFrame(columns=["Date", "var95", "var99"])
    index = 0 

    for i in range(price_history, len(data)):
        if int(data.IsSignal[i]) == 1: 
            mean=data.Close[i-price_history:i].pct_change(1).mean()
            std=data.Close[i-price_history:i].pct_change(1).std()

            simulated = [random.gauss(mean,std) for x in range(shots)]

            simulated.sort(reverse=True)
            var95 = simulated[int(len(simulated)*0.95)]
            var99 = simulated[int(len(simulated)*0.99)]
            output_df.loc[index] = [data.Date[i], var95, var99]
            index += 1
    
    payload = {
      "Status":0, 
      "Data": output_df.to_json()
    }
    
    return json.dumps(payload).encode('utf-8')
