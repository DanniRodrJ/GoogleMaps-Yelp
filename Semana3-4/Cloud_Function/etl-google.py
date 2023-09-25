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
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones

  # Extraer los estados Florida y Pennsylvania
  bucket_name = 'data_cruda'
  florida_path = 'gs://{}/{}'.format(bucket_name, 'Google/Florida.parquet')
  florida = get_read_file(florida_path)
  bucket_name = 'data_extraccion'
  pennsylvania_path = 'gs://{}/{}'.format(bucket_name, 'entry_test/Pennsylvania.parquet')
  pennsylvania = get_read_file(pennsylvania_path)

  # TRANSFORMACION DE LOS 3 ESTADOS
  california['state'] = 'California'
  florida['state'] = 'Florida'
  pennsylvania['state'] = 'Pennsylvania'

  # Columnas a eliminar en los 3 estados
  columnas_a_eliminar = ['pics', 'resp']
  try:
    california = california.drop(columnas_a_eliminar, axis=1)
  except KeyError:
    pass
  try:
    florida = florida.drop(columnas_a_eliminar, axis=1)
  except KeyError:
    pass
  try:
    pennsylvania = pennsylvania.drop(columnas_a_eliminar, axis=1)
  except KeyError:
    pass

  # Cambio del tipo de dato de la columna 'date' en los 3 estados
  try:
    california['time'] = pd.to_datetime(california['time'], unit='ms').dt.date
  except ValueError:
    pass
  try:
    florida['time'] = pd.to_datetime(florida['time'], unit='ms').dt.date
  except ValueError:
    pass
  try:
    pennsylvania['time'] = pd.to_datetime(pennsylvania['time'], unit='ms').dt.date
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

  # Guardamos user como archivo Parquet
  destination_bucket = 'data_extraccion'
  dates_file_path = f'gs://{destination_bucket}/output_test/Google_users.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_users.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar user: {round(save_end_time - save_start_time, 4)} segundos")


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

  # Guardamos dates como archivo Parquet
  destination_bucket = 'data_extraccion'
  dates_file_path = f'gs://{destination_bucket}/output_test/Google_dates.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_dates.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar dates: {round(save_end_time - save_start_time, 4)} segundos")

  # Transformaciones finales en 'reviews'
  reviews = reviews.merge(combined_dates[['date', 'id_date']], left_on='time', right_on='date', how='left')
  reviews = reviews.drop(['date', 'time'], axis=1)
  reviews = reviews[['gmap_id', 'user_id', 'rating', 'text', 'id_date' ,'state']]

  # Concatenar con el archivo limpio de Google_review 
  bucket_name = 'data_limpia'
  review_path = 'gs://{}/{}'.format(bucket_name, 'Google/Google_reviews.parquet')
  reviews_limpio = get_read_file(review_path)
  combined_reviews = pd.concat([review_limpio, reviews])

  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()  
  print(f"Tiempo de transformación de reviews: {round(transform_end_time - transform_start_time, 4)} segundos")

  # Guardamos reviews como archivo Parquet
  destination_bucket = 'data_extraccion'
  output_file_path = f'gs://{destination_bucket}/output_test/Google_reviews.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_reviews.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar reviews: {round(save_end_time - save_start_time, 4)} segundos")

  return reviews # Devolver el DataFrame leído



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

  # Guardamos combined_cat como archivo Parquet
  destination_bucket = 'data_extraccion'
  dates_file_path = f'gs://{destination_bucket}/output_test/Google_link_cat.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_cat.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar link_cat: {round(save_end_time - save_start_time, 4)} segundos")

  # Eliminando la columna 'category' del Dataframe 'metadata_filtrada'
  metadata_filtrada.drop('category', axis=1, inplace=True)

  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()  
  print(f"Tiempo de transformación de metadata: {round(transform_end_time - transform_start_time, 4)} segundos")

  # Guardamos metadata_filtrada como archivo Parquet
  destination_bucket = 'data_extraccion'
  output_file_path = f'gs://{destination_bucket}/output_test/Google_business.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  metadata_filtrada.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar metadata: {round(save_end_time - save_start_time, 4)} segundos")

  return metadata_filtrada  # Devolver el DataFrame leído


@functions_framework.http
def timer_function(request):
  start_time = time.time()  # Registro de tiempo de inicio
  # Nombre del bucket donde serán extraidos los nuevos archivos en Cloud Storage
  bucket_name = 'data_extraccion'
  file_paths = [
        'gs://{}/{}'.format(bucket_name, 'entry_test/california.parquet'),
        'gs://{}/{}'.format(bucket_name, 'entry_test/metadata.json')]

  for file_path in file_paths:
    df = get_read_file(file_path)
    if df is None:
      print(f"Error al leer el archivo: {file_path}")
      end_time = time.time()  # Registro de tiempo de finalización
      total_time = end_time - start_time  # Cálculo del tiempo total de ejecución
      print(f"Tiempo total de ejecución: {total_time} segundos")
      break  # Detener el bucle si df es None

    name = file_path.split('/')[-1].split('.')[-2]
    if name == 'california':
      review = transform_review(df)
    if name == 'metadata':
      metadata = transform_metadata(df, review)

  end_time = time.time()  # Registro de tiempo de finalización
  total_time = end_time - start_time  # Cálculo del tiempo total de ejecución
  print(f"Tiempo total de ejecución: {total_time} segundos")
  return 'Procesamiento de archivos completado'  # Devolver un mensaje de éxito