from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import matplotlib.pyplot as plt

# Configura tu API key
API_KEY = "612145"
symbol = "IAG.MC"  # IAG en la Bolsa de Madrid
symbol = 'NVDA'
# Inicializa la API
ts = TimeSeries(key=API_KEY, output_format='pandas')

# Descarga datos diarios (compact: últimos 100 días)
data, meta_data = ts.get_daily(symbol=symbol, outputsize='full')

# Renombra columnas para facilidad
data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

# Asegura orden cronológico
data = data.sort_index()

# Calcula cambio diario
data['Daily Change (€)'] = data['Close'].diff()

# Plot
plt.figure(figsize=(12, 6))
plt.plot(data.index, data['Daily Change (€)'], label="Cambio diario (€)", color='blue')
plt.title("Cambio diario en el precio de cierre - IAG.MC")
plt.xlabel("Fecha")
plt.ylabel("Cambio (€)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()