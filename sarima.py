import pandas as pd
import numpy as np
from sktime.forecasting.arima import AutoARIMA
from sklearn.linear_model import LinearRegression, Ridge, Lasso, ElasticNet, BayesianRidge
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import mean_squared_error, mean_absolute_error
from statsmodels.tsa.ar_model import AutoReg
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import acf
from statsmodels.tsa.seasonal import seasonal_decompose
import warnings
warnings.filterwarnings("ignore")  # Suppress all warnings

def run_sarima(pred_array, predictedValue, m=None):
    def is_repeating_pattern(series):
        for i in range(1, len(series) // 2 + 1):
            pattern = series[:i]
            if pattern * (len(series) // i) == series:
                return pattern
        return None

    def calculate_rmse(actual, predicted):
        return np.sqrt(mean_squared_error(actual, predicted))

    def calculate_mae(actual, predicted):
        return mean_absolute_error(actual, predicted)

    def calculate_combined_score(rmse, mae, rmse_weight=0.25, mae_weight=0.75):
        combined_score = (rmse_weight * rmse + mae_weight * mae) / (rmse_weight + mae_weight)
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

    def run_linear_models(pred_array, predictedValue, model_type):
        X = np.arange(len(pred_array)).reshape(-1, 1)
        y = np.array(pred_array)

        if model_type == 'Linear Regression':
            model = LinearRegression()
        elif model_type == 'Ridge Regression':
            model = Ridge()
        elif model_type == 'Lasso Regression':
            model = Lasso()
        elif model_type == 'Elastic Net Regression':
            model = ElasticNet()
        elif model_type == 'Bayesian Regression':
            model = BayesianRidge()
        elif model_type == 'Polynomial Regression':
            poly = PolynomialFeatures(degree=2)
            X_poly = poly.fit_transform(X)
            model = LinearRegression()
            model.fit(X_poly, y)
            X_future_poly = poly.transform(np.arange(len(pred_array), len(pred_array) + predictedValue).reshape(-1, 1))
            forecast = model.predict(X_future_poly)
            return [round(p, 3) for p in forecast]

        model.fit(X, y)
        X_future = np.arange(len(pred_array), len(pred_array) + predictedValue).reshape(-1, 1)
        forecast = model.predict(X_future)
        return [round(p, 3) for p in forecast]

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
                    model = ARIMA(series, seasonal_order=(3, 1, 1, seasonality_period)).fit()
                    forecast = model.predict(start=len(series), end=len(series) + predictedValue - 1)
                else:
                    return [100] * predictedValue, float('inf'), float('inf')  # Return high score if seasonality is not detected
            elif model_type == 'SARIMAX':
                model = SARIMAX(series, order=(3, 1, 1), seasonal_order=(3, 1, 1, 12))
                sarima_result = model.fit(disp=False)
                forecast = sarima_result.predict(start=len(series), end=len(series) + predictedValue - 1)
            elif model_type == 'Auto ARIMA':
                model = AutoARIMA(sp=seasonality_period if seasonality_period else 1)
                model.fit(series)
                forecast = model.predict(fh=np.arange(1, predictedValue + 1))

            rmse = np.sqrt(mean_squared_error(series[-min(len(series), len(forecast)):], forecast[:min(len(series), len(forecast))]))
            mae = mean_absolute_error(series[-min(len(series), len(forecast)):], forecast[:min(len(series), len(forecast))])

            combined_score = calculate_combined_score(rmse, mae)
            return [round(p, 3) for p in forecast], rmse, mae, combined_score

        except Exception as e:
            print(f"Model {model_type} failed: {e}")
            return [100] * predictedValue, float('inf'), float('inf')

    model_results = {}
    series = pd.Series(pred_array)

    # Check for repeating pattern
    if len(set(pred_array)) == 1:
        print("Using pattern repetition as model")
        return [pred_array[0]] * predictedValue

    pattern = is_repeating_pattern(pred_array)
    if pattern:
        print("Using pattern repetition as model")
        return (pattern * ((predictedValue // len(pattern)) + 1))[:predictedValue]

    # Detect seasonality only if m is not provided
    if m is None or m is not None:
        seasonality_period = detect_seasonality(series)
    else:
        seasonality_period = m

    # Run regression models
    regression_models = ['Linear Regression', 'Ridge Regression', 'Lasso Regression', 'Elastic Net Regression',
                         'Bayesian Regression', 'Polynomial Regression']

    for model_name in regression_models:
        try:
            forecast = run_linear_models(pred_array, predictedValue, model_name)
            rmse = np.sqrt(mean_squared_error(series[-min(len(series), len(forecast)):], forecast[:min(len(series), len(forecast))]))
            mae = mean_absolute_error(series[-min(len(series), len(forecast)):], forecast[:min(len(series), len(forecast))])
            combined_score = calculate_combined_score(rmse, mae)
            model_results[model_name] = {
                'forecast': forecast,
                'rmse': rmse,
                'mae': mae,
                'combined_score': combined_score
            }
            print(f"Model: {model_name} | RMSE: {rmse} | MAE: {mae} | Combined Score: {combined_score}")
        except Exception as e:
            model_results[model_name] = {'combined_score': float('inf')}

    # Run time series models
    time_series_models = ['AR', 'ARMA', 'ARIMA', 'SARIMA', 'SARIMAX', 'Auto ARIMA']

    for model_name in time_series_models:
        try:
            forecast, rmse, mae, combined_score = run_arima_models(series, predictedValue, model_name, seasonality_period)
            model_results[model_name] = {
                'forecast': forecast,
                'rmse': rmse,
                'mae': mae,
                'combined_score': combined_score
            }
            print(f"Model: {model_name} | RMSE: {rmse} | MAE: {mae} | Combined Score: {combined_score}")
        except Exception as e:
            model_results[model_name] = {'combined_score': float('inf')}

    # Sort models by combined score
    sorted_models = sorted(model_results.items(), key=lambda x: x[1].get('combined_score', float('inf')))

    # Select the best model
    for best_model_name, best_model_data in sorted_models:
        forecast = best_model_data.get('forecast')
        if forecast and len(set(forecast)) > 1 and all(p > 0 for p in forecast):
            print(f"Using {best_model_name} as the model with the lowest valid combined score")
            return forecast

    # If all models fail, fall back to AR model
    forecast, _, _, _ = run_arima_models(pred_array, predictedValue, 'AR')
    print("All models failed; defaulting to AR model")
    return forecast


# Example usage
series = [11,29,29,6,22,23]

predictions = run_sarima(series, predictedValue=35)

print("Predictions:", predictions)
