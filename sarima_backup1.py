import pandas as pd
import numpy as np
from sktime.forecasting.arima import AutoARIMA
from sklearn.metrics import mean_squared_error, mean_absolute_error
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import acf
from statsmodels.tsa.seasonal import seasonal_decompose
import warnings
warnings.filterwarnings("ignore")  # Suppress all warnings

def calculate_aic(model):
    return model.aic if hasattr(model, 'aic') else float('inf')

def calculate_rmse(actual, predicted):
    return np.sqrt(mean_squared_error(actual, predicted))

def calculate_mae(actual, predicted):
    return mean_absolute_error(actual, predicted)

def calculate_combined_score(aic, rmse, mae, aic_weight=0.60, rmse_weight=0.20, mae_weight=0.20):
    combined_score = (aic_weight * aic + rmse_weight * rmse + mae_weight * mae) / (aic_weight + rmse_weight + mae_weight)
    return combined_score

def detect_seasonality(series):
    series = pd.Series(series)
    lag_acf = acf(series, fft=True, nlags=len(series) // 2)
    seasonality_period = np.argmax(lag_acf[1:]) + 1
    if seasonality_period < 2:
        print("No strong seasonality detected.")
        return None

    try:
        decomposition = seasonal_decompose(series, period=seasonality_period, model='additive', extrapolate_trend='freq')
        seasonal = decomposition.seasonal

        if np.max(seasonal) > 0.05:
            print(f"Detected seasonality with period: {seasonality_period}")
            return seasonality_period
        else:
            print("No strong seasonality detected.")
            return None
    except ValueError as e:
        print(f"Seasonality detection failed: {e}")
        return None

def run_arima_models(series, predictedValue, model_type, seasonality_period=None):
    try:
        if model_type == 'AR':
            model = AutoReg(series, lags=1).fit()
            forecast = model.predict(start=len(series), end=len(series) + predictedValue - 1)
        elif model_type == 'ARMA':
            model = ARIMA(series, order=(1, 0, 1)).fit()
            forecast = model.predict(start=len(series), end=len(series) + predictedValue - 1)
        elif model_type == 'ARIMA':
            model = ARIMA(series, order=(1, 1, 1)).fit()
            forecast = model.predict(start=len(series), end=len(series) + predictedValue - 1)
        elif model_type == 'SARIMA':
            if seasonality_period:
                model = ARIMA(series, seasonal_order=(1, 1, 1, seasonality_period)).fit()
                forecast = model.predict(start=len(series), end=len(series) + predictedValue - 1)
            else:
                print(f"Skipping {model_type}: No seasonality detected.")
                return None, float('inf'), float('inf'), float('inf')  # Return invalid metrics if no seasonality
        elif model_type == 'Auto ARIMA':
            model = AutoARIMA(sp=seasonality_period if seasonality_period else 1)
            model.fit(series)
            forecast = model.predict(fh=np.arange(1, predictedValue + 1))

        # Check for forecast validity
        if np.any(np.isnan(forecast)) or len(set(forecast)) == 1:
            print(f"{model_type} model returned invalid or constant forecast.")
            return None, float('inf'), float('inf'), float('inf')

        # Calculate metrics
        aic = calculate_aic(model)
        rmse = calculate_rmse(series[-len(forecast):], forecast)
        mae = calculate_mae(series[-len(forecast):], forecast)
        combined_score = calculate_combined_score(aic, rmse, mae)
        return [round(p, 3) for p in forecast], aic, rmse, mae, combined_score

    except Exception as e:
        print(f"Error in {model_type}: {e}")
        return None, float('inf'), float('inf'), float('inf')  # Return invalid metrics in case of failure

def run_sarima(pred_array, predictedValue=11, m=None):
    model_results = {}
    series = pd.Series(pred_array)

    if len(set(pred_array)) == 1:
        print("Using pattern repetition as model")
        return [pred_array[0]] * predictedValue

    # Detect seasonality
    if m is None:
        seasonality_period = detect_seasonality(series)
        m = seasonality_period

    # Time Series Models
    time_series_models = ['AR', 'ARMA', 'ARIMA', 'SARIMA', 'Auto ARIMA']

    for model_name in time_series_models:
        try:
            forecast, aic, rmse, mae, combined_score = run_arima_models(series, predictedValue, model_name, seasonality_period)
            if forecast is not None:
                model_results[model_name] = {
                    'forecast': forecast,
                    'aic': aic,
                    'rmse': rmse,
                    'mae': mae,
                    'combined_score': combined_score
                }
                print(f"Model: {model_name} | RMSE: {rmse} | MAE: {mae} | AIC: {aic} | Combined Score: {combined_score}")
        except Exception as e:
            model_results[model_name] = {'combined_score': float('inf')}

    # Sort by best combined score
    sorted_models = sorted(model_results.items(), key=lambda x: x[1].get('combined_score', float('inf')))

    # If no valid models found, return None to handle later
    if len(sorted_models) == 0:
        print("All models failed.")
        return None

    for best_model_name, best_model_data in sorted_models:
        forecast = best_model_data.get('forecast')
        if forecast and len(set(forecast)) > 1 and all(p > 0 for p in forecast):
            print(f"Using {best_model_name} as the model with the lowest valid combined score")
            return forecast

    print("All models failed.")
    return None

# Example usage
series = [29, 29, 6, 22, 23]
predictions = run_sarima(series, predictedValue=3)
if predictions is not None:
    print("Predictions:", predictions)
else:
    print("No valid predictions generated.")

