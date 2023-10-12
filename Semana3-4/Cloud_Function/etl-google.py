from google.cloud import storage
import functions_framework
import pandas as pd
import numpy as np
import gcsfs
import time
import re
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def get_read_file(file_path):
  # Verifica el tipo de archivo y utiliza Pandas para leerlo
  file_extension = file_path.rsplit('.', 1)[-1].lower()
  name = file_path.split('/')[-1].split('.')[-2]

  if file_extension == 'csv':
    read_start_time = time.time()  # Registro de tiempo antes de la lectura
    df = pd.read_csv(file_path)
    read_end_time = time.time()  # Registro de tiempo después de la lectura
  elif file_extension == 'json':
    try:
      read_start_time = time.time()  # Registro de tiempo antes de la lectura
      df = pd.read_json(file_path)
      read_end_time = time.time()  # Registro de tiempo después de la lectura
    except ValueError as message:
      if 'Trailing data' in str(message):
        read_start_time = time.time()  # Registro de tiempo antes de la lectura
        df = pd.read_json(file_path, lines=True)
        read_end_time = time.time()  # Registro de tiempo después de la lectura
      else:
        print('Ocurrió un error cargando el archivo JSON:', message)
        return None  # Devolver None en caso de error
  elif file_extension == 'parquet':
    try:
      read_start_time = time.time()  # Registro de tiempo antes de la lectura
      df = pd.read_parquet(file_path)
      read_end_time = time.time()  # Registro de tiempo después de la lectura
    except TypeError as message:
      if 'NAType' in str(message):
        read_start_time = time.time()  # Registro de tiempo antes de la lectura
        df = pd.read_parquet(file_path, use_nullable_dtypes=False)
        read_end_time = time.time()  # Registro de tiempo después de la lectura
      else:
        print('Ocurrió un error cargando el archivo PARQUET:', message)
        return None  # Devolver None en caso de error 
  else:
    print(f'Formato de archivo {file_extension} no compatible')
    return None  # Devolver None en caso de que no haya un formato compatible

  name = file_path.split('/')[-1].split('.')[-2]  # Obtiene el nombre del archivo
  print(f"Tiempo de lectura del archivo {name}: {round(read_end_time - read_start_time, 4)} segundos")

  return df # Devolver el DataFrame leído


def transform_review(california):
  transform_start_time_r = time.time()  # Registro de tiempo antes de las transformaciones

  # Extraer los estados Florida y Pennsylvania
  bucket_name = 'data_extraccion'
  florida_path = 'gs://{}/{}'.format(bucket_name, 'entry_test/Florida.parquet')
  florida = get_read_file(florida_path)
  bucket_name = 'data_extraccion'
  pennsylvania_path = 'gs://{}/{}'.format(bucket_name, 'entry_test/Pennsylvania.parquet')
  pennsylvania = get_read_file(pennsylvania_path)

  # TRANSFORMACION DE LOS 3 ESTADOS
  california['state'] = 'California'
  florida['state'] = 'Florida'
  pennsylvania['state'] = 'Pennsylvania'
  
  dataframes = [california, florida, pennsylvania]

  # Columnas a eliminar en los 3 estados
  columnas_a_eliminar = ['pics', 'resp']

  for df in dataframes:
    try:
      df.drop(columnas_a_eliminar, axis=1, inplace=True)
    except KeyError:
      pass

  # Cambio del tipo de dato de la columna 'date' en los 3 estados
  for df in dataframes:
    try:
      df['time'] = pd.to_datetime(df['time'], unit='ms').dt.date
    except ValueError:
      pass

  # Concatenar los 3 estados
  reviews = pd.concat([california, florida, pennsylvania], ignore_index=True)
  print('Concatenados los archivos reviews de los 3 estados')

  # Filtramos la data, para obtener los datos con fecha superior al año 2018
  reviews['time'] = pd.to_datetime(reviews['time'])
  reviews = reviews[reviews['time'].dt.year >= 2018]
  conteo_por_gmap_id = reviews.groupby('gmap_id').size()
  gmap_id_con_mas_de_1000_registros = conteo_por_gmap_id[conteo_por_gmap_id > 1000].index
  reviews = reviews[reviews['gmap_id'].isin(gmap_id_con_mas_de_1000_registros)]
  reviews.drop_duplicates(inplace=True)
  
  # Verificar si en el DataFrame 'reviews' hay data nueva y/o existente, y quedarnos con solo la nueva
  bucket_name = 'data_limpia'
  review_path = 'gs://{}/{}'.format(bucket_name, 'Google/Google_reviews.parquet')
  reviews_limpio = get_read_file(review_path)
  
  def verify_new_records(df_received, df_clean):
    """
    Verifica los registros nuevos entre dos DataFrames.
    Args:
      df_received: El DataFrame con los datos recibidos.
      df_clean: El DataFrame con los datos limpios.

    Return:
      El DataFrame con los registros nuevos.
    """
    # Realizar el merge entre los archivos "df_received" y "df_clean" basado en "gmap_id"
    df_merged = df_received.merge(df_clean, how='inner', on=['gmap_id'])
    # Los registros que no coinciden son los registros nuevos.
    new_records = df_received[~df_received.index.isin(df_merged.index)]
    
    return new_records
  
  # Se llama a la función que verifica los registros nuevos y los almacena en 'business'
  reviews = verify_new_records(reviews, reviews_limpio)
  
  if reviews.empty:
    print(f"No hay data nueva en: reviews")
    
  # Registro de tiempo después de los filtros realizados
  transform_end_time = time.time()
  print(f"Tiempo de transformación para filtrar los 3 estados por fecha: {round(transform_end_time - transform_start_time_r, 4)} segundos")


  # TRANSFORMACIONES A PARTIR DE REVIEWS
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones


  # CREAR UN NUEVO DATAFRAME 'user' CON LAS COLUMNAS 'user_id' y 'name'
  users = reviews[['user_id', 'name']]
  # Eliminación de la columna 'name'
  reviews = reviews.drop('name', axis=1)

  # Extraer el archivo Google_user existente de la data limpia
  bucket_name = 'data_limpia'
  users_path = 'gs://{}/{}'.format(bucket_name, 'Google/Google_users.parquet')
  users_limpio = get_read_file(users_path)

  # Concatenar las fechas nuevas al DataFrame de fechas existente
  combined_users = pd.concat([users_limpio, users])
  combined_users['user_id'] = combined_users['user_id'].astype(float)
  combined_users['name'] = combined_users['name'].astype(str)
  combined_users.drop_duplicates(inplace = True)

  # Guardamos y actualizamos user como archivo Parquet
  destination_bucket = 'data_limpia'
  dates_file_path = f'gs://{destination_bucket}/Google/Google_users.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_users.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar user: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de user como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  hours_file_path = f'gs://{destination_bucket}/Google/Google_users.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  users.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar users (new): {round(save_end_time - save_start_time, 4)} segundos")
  

  # CREAR UN NUEVO DATAFRAME 'dates' 
  # Extraer el archivo 'dates' limpio de Yelp para obtener el último id registrado
  bucket_name = 'data_limpia'
  input_file_path = 'gs://{}/{}'.format(bucket_name, 'Google/Google_dates.parquet')
  dates = get_read_file(input_file_path)
  # Obtener el máximo valor actual de id_date en el DataFrame existente
  max_id_date = dates['id_date'].max()
 
  # Obtenemos las fechas únicas de los registros
  new_dates = pd.DataFrame({'date': reviews['time'].unique()})
  new_dates = new_dates.sort_values(by='date')

  # Filtrar 'dates' y 'new_dates' para verificar si hay fechas nuevas
  fechas_nuevas = new_dates[~new_dates['date'].isin(dates['date'])]
  # Generar los nuevos id's para las fechas nuevas a partir de 'max_id_date'
  fechas_nuevas.loc[:, 'id_date'] = range(dates['id_date'].max()+1, max_id_date+1+len(fechas_nuevas))

  # Concatenar las fechas nuevas al DataFrame de fechas existente
  combined_dates = pd.concat([dates, fechas_nuevas])

  # Guardamos y actualizamos dates como archivo Parquet
  destination_bucket = 'data_limpia'
  dates_file_path = f'gs://{destination_bucket}/Google/Google_dates.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_dates.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar dates: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de dates como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  hours_file_path = f'gs://{destination_bucket}/Google/Google_dates.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  fechas_nuevas.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar dates (new): {round(save_end_time - save_start_time, 4)} segundos")
  
  # Registro de tiempo después de las transformaciones a partir de reviews
  transform_end_time = time.time()  
  print(f"Tiempo de transformación a partir de reviews: {round(transform_end_time - transform_start_time, 4)} segundos")

  # Transformaciones finales en 'reviews'
  reviews = reviews.merge(combined_dates[['date', 'id_date']], left_on='time', right_on='date', how='left')
  reviews = reviews.drop(['date', 'time'], axis=1)
  reviews = reviews[['gmap_id', 'user_id', 'rating', 'text', 'id_date' ,'state']]

  # Registro de tiempo después de las transformación de reviews
  transform_end_time = time.time()  
  print(f"Tiempo de transformación de reviews: {round(transform_end_time - transform_start_time_r, 4)} segundos")


  # Concatenar con el archivo limpio de Google_review que fue almacenado en 'reviews_limpio'
  combined_reviews = pd.concat([reviews_limpio, reviews])
  
  # Guardamos y actualizamos reviews como archivo Parquet
  destination_bucket = 'data_limpia'
  output_file_path = f'gs://{destination_bucket}/Google/Google_reviews.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_reviews.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar reviews: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de reviews como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  hours_file_path = f'gs://{destination_bucket}/Google/Google_reviews.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  reviews.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar reviews (new): {round(save_end_time - save_start_time, 4)} segundos")
  
  # ESTAS LINEAS DE CÓDIGO FUNCIONAN SI YA SE TIENE UN REGISTRO EN 'carga_incremental/data_historica'
  fecha_actual = datetime.datetime.now().date()  # Obtener la fecha actual
  # Crear un nuevo DataFrame con las columnas 'gmap_id' y 'fecha_actual'
  nuevo_df = pd.DataFrame({
    'gmap_id': reviews['business_id'],
    'fecha_actual': fecha_actual})
  # Extraer el archivo Google existente de la data historica
  bucket_name = 'carga_incremental'
  google_path = 'gs://{}/{}'.format(bucket_name, 'data_historica/Google.parquet')
  google = get_read_file(google_path)
  # Concatenar google new al DataFrame existente
  combined_hist = pd.concat([google, nuevo_df])
  combined_hist['gmap_id'].drop_duplicates(inplace = True)
  
  # Guardamos los registros id's nuevos de reviews
  destination_bucket = 'carga_incremental'
  output_file_path = f'gs://{destination_bucket}/data_historica/Google.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_hist.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar el registro histórico de Google (new): {round(save_end_time - save_start_time, 4)} segundos")

  # Se retorna el Dataframe con data nueva de reviews, para ser usada en la otras función
  return reviews 


def transform_metadata(df, reviews):
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones

  # Función para convertir los registros de la columna "category" en valores separados por comas
  def join_categories(row):
    if row is not None:
      return ','.join(row)
    else:
      return None
  # Aplicar la función a la columna "category" utilizando apply y lambda
  df['category'] = df['category'].apply(lambda x: join_categories(x))


  # Filtro por rubro
  target_categories = ['restaurant', 'coffee', 'rice', 'paan', 'ice cream', 'tortilla', 'tofu', 'pie', 'soup',
                        'cheese', 'cupcake', 'pasta', 'cookie', 'chocolate', 'frozen yogurt', 'salad', 'cake',
                        'donut', 'sandwich', 'chicken', 'pizza', 'burguer', 'hot dog']
  target_categories_lower = [category.lower() for category in target_categories]
  # Filtrar los registros que no tienen valores nulos en la columna 'Categories'
  metadata_category = df.dropna(subset=['category'])
  # Filtrar los registros que contienen alguna de las categorías objetivo en la columna "Categories"
  metadata_filtrada = metadata_category[metadata_category['category'].str.lower().str.contains('|'.join(target_categories_lower))]

  # Eliminación de columnas y filas duplicadas en 'gmap_id'
  metadata_filtrada = metadata_filtrada.drop(['avg_rating', 'num_of_reviews', 'address', 'price', 'relative_results', 'url', 'state'], axis=1) 
  metadata_filtrada = metadata_filtrada.drop_duplicates(subset='gmap_id', keep='first')


  # FILTRO DE METADATA POR LOS 3 ESTADOS (CALIFORNIA, FLORIDA, PENNSYLVANIA)
  gmap_ids_en_reviews = reviews['gmap_id'].unique()
  metadata_filtrada = metadata_filtrada[metadata_filtrada['gmap_id'].isin(gmap_ids_en_reviews)]
  metadata_filtrada.reset_index(drop=True, inplace=True)
  print('Metadata filtrada por los 3 estados')
  
  # Se eliminan las columnas 'MISC' y 'miscellaneous'
  metadata_filtrada = metadata_filtrada.drop(['MISC'], axis=1)


  # TRANSFORMACIONES A LA COLUMNA 'category'
  bucket_name = 'data_limpia'
  input_file_path = 'gs://{}/{}'.format(bucket_name, 'Google/categories.parquet')
  df_categories = get_read_file(input_file_path)

  dataframes_list = []

  for index, row in metadata_filtrada.iterrows():
    gmap_id = row['gmap_id']
    categories = row['category'].split(', ')

    for category in categories:
      matching_category = df_categories[df_categories['description'] == category]

      if not matching_category.empty:
        id_cat = matching_category['id_cat'].values[0]
        dataframes_list.append(pd.DataFrame({'gmap_id': [gmap_id], 'id_cat': [id_cat]}))

  df_met_cat = pd.concat(dataframes_list, ignore_index=True)
  df_met_cat.drop_duplicates(inplace=True)

  # Extraer el archivo link_cat existente de la data limpia
  bucket_name = 'data_limpia'
  link_cat_path = 'gs://{}/{}'.format(bucket_name, 'Google/link_cat.parquet')
  link_cat = get_read_file(link_cat_path)

  # Concatenar las fechas nuevas al DataFrame de fechas existente
  combined_cat = pd.concat([link_cat, df_met_cat])
  combined_cat['gmap_id'].drop_duplicates(inplace = True)

  # Asegurar los tipos de datos antes de guardar en parquet
  combined_cat['gmap_id'] = combined_cat['gmap_id'].astype(str)
  combined_cat['cat_id'] = combined_cat['cat_id'].astype(int)

  # Guardamos y actualizamos combined_cat como archivo Parquet
  destination_bucket = 'data_limpia'
  dates_file_path = f'gs://{destination_bucket}/Google/Google_link_cat.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_cat.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar link_cat: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de link_cat como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  hours_file_path = f'gs://{destination_bucket}/Google/Google_link_cat.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  df_met_cat.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar link_cat (new): {round(save_end_time - save_start_time, 4)} segundos")

  # Eliminando la columna 'category' del Dataframe 'metadata_filtrada'
  metadata_filtrada.drop('category', axis=1, inplace=True)

  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()  
  print(f"Tiempo de transformación de metadata: {round(transform_end_time - transform_start_time, 4)} segundos")


  # Extraer el archivo metadata existente de la data limpia
  bucket_name = 'data_limpia'
  metadata_path = 'gs://{}/{}'.format(bucket_name, 'Google/Google_business.parquet')
  metadata = get_read_file(metadata_path)
  
  # Concatenar con el archivo limpio de Google_business
  combined_metadata = pd.concat([metadata, metadata_filtrada])

  # Guardamos y actualizamos metadata_filtrada como archivo Parquet
  destination_bucket = 'data_limpia'
  output_file_path = f'gs://{destination_bucket}/Google/Google_business.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_metadata.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar metadata: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de metadata_filtrada como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  hours_file_path = f'gs://{destination_bucket}/Google/Google_business.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  metadata_filtrada.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar metadata (new): {round(save_end_time - save_start_time, 4)} segundos")

  return metadata_filtrada


# Decorador para registrar una función como HTTP
@functions_framework.http
def timer_function(request):
  start_time = time.time()  # Registro de tiempo de inicio
  # Nombre del bucket donde serán extraídos los nuevos archivos en Cloud Storage
  bucket_name = 'data_extraccion'
  file_paths = [
        'gs://{}/{}'.format(bucket_name, 'entry_test/california.parquet'),
        'gs://{}/{}'.format(bucket_name, 'entry_test/metadata.json')]

  # Definir las variables fuera del bucle
  review = None
  metadata = None
  
  # Bucle que lee todos los archivos y llama a las funciones para las transformaciones
  for file_path in file_paths:
    df = get_read_file(file_path)
    if df is None:
      print(f"Error al leer el archivo: {file_path}")
      end_time = time.time()  # Registro de tiempo de finalización
      total_time = end_time - start_time  # Cálculo del tiempo total de ejecución
      print(f"Tiempo total de ejecución: {total_time} segundos")
      break  # Detener el bucle si df es None

    # Verifica el nombre del archivo para llamar a su respectiva función 
    name = file_path.split('/')[-1].split('.')[-2]
    if name == 'california':
      review = transform_review(df)
    if name == 'metadata':
      metadata = transform_metadata(df, review)

  end_time = time.time()  # Registro de tiempo de finalización
  total_time = end_time - start_time  # Cálculo del tiempo total de ejecución
  print(f"Tiempo total de ejecución: {total_time} segundos")
  return 'Procesamiento de archivos completado'  # Devolver un mensaje de éxito