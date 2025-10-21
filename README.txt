 Repositorio de preparación de datos 

Archivos incluidos:
- ventas_tienda_raw.csv   : Dataset original (50+ registros) con errores simulados.
- ventas_tienda_clean.csv : Dataset limpio resultante del proceso ETL.
- etl_tienda.py           : Script en Python (pandas) que realiza el ETL.
- README.txt              : Este archivo.

Cómo ejecutar el ETL localmente:
1) Asegúrate de tener Python 3 y pandas instalados:
   pip install pandas

2) Coloca 'ventas_tienda_raw.csv' y 'etl_tienda.py' en la misma carpeta y ejecuta:
   python etl_tienda.py

El script generará 'ventas_tienda_clean.csv' con las transformaciones descritas:
- Normaliza formatos de fecha.
- Convierte y corrige unit_price (valores negativos o faltantes).
- Limpia nombres de producto y ciudades (normalización de texto).
- Elimina duplicados por sale_id y registros con campos críticos faltantes.
- Recalcula total_amount como unit_price * quantity.
- Aplica capping a outliers extremos.
