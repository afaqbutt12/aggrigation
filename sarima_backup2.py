from statsmodels.tsa.statespace.sarimax import SARIMAX
import pandas as pd
import numpy as np


def run_sarima(pred_array, predictedValue=11, m=12):

    print("Array", pred_array)

    # Define the series
    series = pd.Series(pred_array)
    # m = 12
    # Fitting the SARIMA model
    if m == 0:
        model = SARIMAX(series, order=(1,1,1) , seasonal_order=(0,0,0,0))
    else:
        model = SARIMAX(series, order=(1,1,1) , seasonal_order=(1,1,1,m))

    # model = SARIMAX(series, order=(1, 1, 1), seasonal_order=(1, 1, 1, m))
    model_fit = model.fit(disp=False)
    # Predicting the next 36 values
    predictions = model_fit.predict(len(series) + 1, len(series) + predictedValue)
    predictions = [int(0 if p <= 0 else p) for p in predictions]
    # predicted_dict = predictions
    # print("predicted_dict :: ", list(predicted_dict))

    return predictions



    # series = pd.Series(pred_array)

    # # Log transformation
    # log_data = np.log(series + 1)  # Adding 1 to avoid log(0) issues

    # # Fitting the SARIMA model
    # if m == 0:
    #     model = SARIMAX(log_data, order=(1,1,1) , seasonal_order=(0,0,0,0))
    # else:
    #     model = SARIMAX(log_data, order=(1,1,1) , seasonal_order=(1,1,1,m))

    # model_fit = model.fit(disp=False)

    # # Predicting the next 3 values
    # predictions = model_fit.predict(len(log_data)+1, len(log_data) + 36)
    # predictions = np.exp(predictions)  # Reverse the log transformation
    # predicted_dict = predictions.to_dict()
    
    # print("predictions :", predicted_dict)
    # return predicted_dict



    # print(predictions)
    #- order=(1,1,1): p=1 (autoregressive term), d=1 (degree of differencing), q=1 (moving average term)
    #- seasonal_order=(1,1,1,12): P=1 (seasonal autoregressive term), D=1 (seasonal degree of differencing), 
    #- Q=1 (seasonal moving average term), S=12 (seasonal period)

    #  Monthly data: m = 12 (12 months in a year)
    # Quarterly data: m = 4 (4 quarters in a year)
    # Semi-annual data: m = 2 (2 halves in a year)
    # Annual data: seasonal order= (0,0,0,0) 


    # old version of non negative
    # series = pd.Series(pred_array)

    # log_data = np.log(series)
    # m=12
    # # Fitting the SARIMA model
    # model = SARIMAX(log_data, order=(1,1,1) , seasonal_order=(0,0,0,0))
    # model_fit = model.fit(disp=False)
    # # Predicting the next 3 values
    # predictions = model_fit.predict(len(log_data), len(log_data)+predictedValue)
    # predicted_dict = np.exp(predictions)
    # print(predicted_dict)


    # old sarima
    # series = pd.Series(pred_array)

    # # Fitting the SARIMA model
    # model = SARIMAX(series, order=(1,1,1) , seasonal_order=(1,1,2,2))

    # model_fit = model.fit(disp=False)

    # # Predicting the next 3 values
    # predictions = model_fit.predict(len(series), len(series)+predictedValue)

    # predicted_dict = predictions.to_dict()
    
    # print("predictions :", predicted_dict)
    # return predicted_dict

    

