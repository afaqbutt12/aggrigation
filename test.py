from sktime.forecasting.arima import AutoARIMA
import numpy as np

y = np.random.rand(100)
model = AutoARIMA(sp=12)
model.fit(y)
predictions = model.predict(fh=np.arange(1, 11))
print(predictions)
