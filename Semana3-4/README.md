## 💾Data Lake

En la Plataforma de Google Cloud, se utilizó Cloud Storage en el cual se establecieron 4 buckets.

1. Bucket "data_cruda" para la data proporcionada por el cliente
2. Bucket "data_clean" donde se almacena la data limpia producto del ETL completo
3. Bucket carga_incremental utilizado como prueba para verificar que la carga de los registros nuevos se realizó correctamente.
4. Bucket "data_extraccion" el cual recibirá toda la data nueva semanalmente.

![Cloud_Storage](../Imagenes/Cloud_Storage.png)

## Modelo Entidad-Relación

## 🛢️​Data Warehouse

El Data Warehouse se creó a través de BigQuery, el cual recibe los archivos del bucket "data_limpia" luego del proceso ETL retornando 2 schemas uno llamado "Tabla_Google" y otro "Tabla_Yelp". De los cuales cada uno de ellos está conformado por las distintas tablas expresadas en el modelo ER.

Por lo tanto, a través de estos schemas se puede extraer la información con consultas SQL, conectándose al cliente de Big Query.

![BigQuery](../Imagenes/BigQuery.png)

## 💡⚙️ ETL automatizado con Cloud Functions y Cloud Scheduler

Para automatizar el proceso ETL cada vez que llegue data nueva al bucket "data_extraccion", se crearon dos funciones en Cloud Functions llamadas "etl-yelp" y "etl-google", las cuales se ejecutan una vez que son llamadas por Cloud Scheduler (actúa como temporizador) semanalmente.

![Cloud_Functions](../Imagenes/Cloud_Functions.png)

![Google_Scheluder](../Imagenes/Google_Scheluder.png)