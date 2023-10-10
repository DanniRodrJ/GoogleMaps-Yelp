from google.cloud import storage
import functions_framework
import pandas as pd
import numpy as np
import gcsfs
import time
import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def get_read_file(file_path):
  # Verifica el tipo de archivo y utiliza Pandas para leerlo
  file_extension = file_path.rsplit('.', 1)[-1].lower()
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
    read_start_time = time.time()  # Registro de tiempo antes de la lectura
    df = pd.read_parquet(file_path)
    read_end_time = time.time()  # Registro de tiempo después de la lectura
  else:
    print(f'Formato de archivo {file_extension} no compatible')
    return None  # Devolver None en caso de que no haya un formato compatible

  name = file_path.split('/')[-1].split('.')[-2]  # Obtiene el nombre del archivo
  print(f"Tiempo de lectura del archivo {name}: {round(read_end_time - read_start_time, 4)} segundos")

  return df # Devolver el DataFrame leído


def transform_business_review(df):
  transform_start_time_b = time.time()  # Registro de tiempo antes de las transformaciones de business

  # Filtro por rubro y por estados
  target_categories = ['restaurant', 'coffee', 'rice', 'paan', 'ice cream', 'tortilla', 'tofu', 'pie', 'soup',
                        'cheese', 'cupcake', 'pasta', 'cookie', 'chocolate', 'frozen yogurt', 'salad', 'cake',
                        'donut', 'sandwich', 'chicken', 'pizza', 'burguer', 'hot dog']
  target_categories_lower = [category.lower() for category in target_categories]
  # Filtrar los registros que no tienen valores nulos en la columna 'Categories'
  df_business_filtered = df.dropna(subset=['categories'])
  # Filtrar los registros que contienen alguna de las categorías objetivo en la columna "Categories"
  df_filteredy = df_business_filtered[df_business_filtered['categories'].str.lower().str.contains('|'.join(target_categories_lower))]
  # Lista con los estados que se analizaran
  estados_interes = ['FL', 'PA', 'CA']
  df_filtrado = df_filteredy[df_filteredy['state'].isin(estados_interes)]
  # Eliminación de filas duplicadas
  columns_to_check = ['business_id', 'name', 'address', 'city', 'state', 'postal_code', 'latitude', 'longitude']
  deduplicated_df = df_filtrado.drop_duplicates(subset=columns_to_check)
  
  # Verificar si en el DataFrame 'business' hay data nueva y/o existente, y quedarnos con solo la nueva
  bucket_name = 'data_limpia'
  business_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/business.parquet')
  business_limpio = get_read_file(business_path)
  
  def verify_new_records(df_received, df_clean):
    """
    Verifica los registros nuevos entre dos DataFrames.
    Args:
      df_received: El DataFrame con los datos recibidos.
      df_clean: El DataFrame con los datos limpios.

    Return:
      El DataFrame con los registros nuevos.
    """
    df_clean.drop(['Date_received'], axis=1, inplace=True)
    # Realizar el merge entre los archivos "df_received" y "df_clean" basado en "business_id"
    df_merged = df_received.merge(df_clean, how='inner', on=['business_id'])
    # Los registros que no coinciden son los registros nuevos.
    new_records = df_received[~df_received.index.isin(df_merged.index)]
    
    return new_records
  
  # Se llama a la función que verifica los registros nuevos y los almacena en 'business'
  business = verify_new_records(deduplicated_df, business_limpio)
  
  if business.empty:
    print(f"No hay data nueva en: business")
    
    
  transform_start_time_r = time.time()  # Registro de tiempo antes de las transformaciones de reviews
  # Extraer el archivo reviews
  bucket_name = 'data_extraccion'
  review_path = 'gs://{}/{}'.format(bucket_name, 'entry_test/review.parquet')
  review = get_read_file(review_path)

  # Convertir la columna 'date' a tipo DateTime
  review['date'] = pd.to_datetime(review['date'])
  # Actualizar la columna 'date' para que solo contenga la fecha
  review['date'] = review['date'].dt.date
  # Fecha mínima con la que vamos a trabajar
  fecha_referencia = pd.to_datetime('2018-01-01').date()
  # Filtramos 'review' para obtener todas las fechas superiores a la de la referencia 
  review_filtrado = review[review['date'] > fecha_referencia]
  review_filtrado = review_filtrado.reindex()

  # Merge para obtener un dataframe de business con los registros coincidentes en reviews
  business_final = business.merge(review_filtrado[['business_id']], on='business_id', how='inner')
  # Eliminar filas duplicadas basadas en el 'business_id'
  business_final.drop_duplicates(subset='business_id', inplace=True)
  print('Business filtrado por rubro, por estado y por fecha')

  # Merge para obtener un dataframe de reviews con los registros coincidentes en business
  review_final = review_filtrado[review_filtrado["business_id"].isin(business["business_id"])]
  review_final = review_final.merge(business[["business_id", "state"]], on="business_id", how="inner")
  print('Review filtrado por rubro, por estado y por fecha')  

  # Registro de tiempo después de los filtros realizados
  transform_end_time = time.time()
  print(f"Tiempo de transformación para los 3 filtros: {round(transform_end_time - transform_start_time_b, 4)} segundos")
  
  
  # TRANSFORMACIONES A PARTIR DE BUSINESS
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones


  # CREAMOS UN NUEVO DATAFRAME CON LA COLUMNA 'Hours'
  df_hours = business_final[['business_id']].join(business_final['hours'].apply(lambda x: pd.Series(x, dtype='object')))
  df_hours.fillna('', inplace=True)

  # Extraer el archivo hours existente de la data limpia
  bucket_name = 'data_limpia'
  hours_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/hours.parquet')
  hours = get_read_file(hours_path)

  # Concatenar los horarios nuevos al DataFrame de horarios existente
  combined_hours = pd.concat([hours, df_hours])
  combined_hours['business_id'].drop_duplicates(inplace = True)
  combined_hours = combined_hours.apply(lambda x: x.astype(str))

  # Guardamos y actualizamos hours como archivo Parquet, en el bucket 'data_limpia'
  destination_bucket = 'data_limpia'
  hours_file_path = f'gs://{destination_bucket}/Yelp/hours.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_hours.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar hours: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de hours como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  hours_file_path = f'gs://{destination_bucket}/Yelp/hours.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  df_hours.to_parquet(hours_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar hours (new): {round(save_end_time - save_start_time, 4)} segundos")


  # CREAR UN NUEVO DATAFRAME CON LA COLUMNA 'categories' Y UN LINKED ENTRE 'business' y 'categories'
  bucket_name = 'data_limpia'
  input_file_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/categories.parquet')
  df_categories = get_read_file(input_file_path)

  dataframes_list = []

  for index, row in business_final.iterrows():
    business_id = row['business_id']
    categories = row['categories'].split(', ')

    for category in categories:
      matching_category = df_categories[df_categories['category'] == category]

      if not matching_category.empty:
        category_id = matching_category['category_id'].values[0]
        dataframes_list.append(pd.DataFrame({'business_id': [business_id], 'category_id': [category_id]}))

  df_bus_cat = pd.concat(dataframes_list, ignore_index=True)
  df_bus_cat.drop_duplicates(inplace=True)

  # Extraer el archivo bus_cat existente de la data limpia
  bucket_name = 'data_limpia'
  bus_cat_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/bus_cat.parquet')
  bus_cat = get_read_file(bus_cat_path)

  # Concatenar bus_cat new al DataFrame existente
  combined_bus_cat = pd.concat([bus_cat, df_bus_cat])
  combined_bus_cat['business_id'].drop_duplicates(inplace = True)

  # Asegurar los tipos de datos antes de guardar en parquet
  combined_bus_cat['business_id'] = combined_bus_cat['business_id'].astype(str)
  combined_bus_cat['category_id'] = combined_bus_cat['category_id'].astype(int)

  # Guardamos y actualizamos bus_cat como archivo Parquet, en el bucket 'data_limpia'
  destination_bucket = 'data_limpia'
  bus_cat_file_path = f'gs://{destination_bucket}/Yelp/bus_cat.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_bus_cat.to_parquet(bus_cat_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar bus_cat: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de bus_cat como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  bus_cat_file_path = f'gs://{destination_bucket}/Yelp/bus_cat.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  df_bus_cat.to_parquet(bus_cat_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar bus_cat (new): {round(save_end_time - save_start_time, 4)} segundos")

  # Registro de tiempo después de las transformaciones a partir de business
  transform_end_time = time.time()
  print(f"Tiempo de transformación a partir de business: {round(transform_end_time - transform_start_time, 4)} segundos")

  # Eliminar las columnas que no serán utilizadas
  business_final.drop(columns=['postal_code', 'stars', 'review_count', 'is_open', 'attributes', 'categories', 'hours'], inplace=True)
  # Concatenar con el archivo limpio de business 
  combined_business = pd.concat([business_limpio, business_final])
  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()
  print(f"Tiempo de transformación total de business: {round(transform_end_time - transform_start_time_b, 4)} segundos")


  # Guardamos y actualizamos combined_business como archivo Parquet, en el bucket 'data_limpia'
  destination_bucket = 'data_limpia'
  output_file_path = f'gs://{destination_bucket}/Yelp/business.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_business.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar business: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de business como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  output_file_path = f'gs://{destination_bucket}/Yelp/business.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  business_final.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar business (new): {round(save_end_time - save_start_time, 4)} segundos")
  
  
  # ESTAS LINEAS DE CÓDIGO FUNCIONAN SI YA SE TIENE UN REGISTRO EN 'carga_incremental/data_historica'
  fecha_actual = datetime.datetime.now().date()  # Obtener la fecha actual
  # Crear un nuevo DataFrame con las columnas 'business_id' y 'fecha_actual'
  nuevo_df = pd.DataFrame({
    'business_id': business['business_id'],
    'fecha_actual': fecha_actual})
  # Extraer el archivo Yelp existente de la data historica
  bucket_name = 'carga_incremental'
  yelp_path = 'gs://{}/{}'.format(bucket_name, 'data_historica/Yelp.parquet')
  yelp = get_read_file(yelp_path)
  # Concatenar yelp new al DataFrame existente
  combined_hist = pd.concat([yelp, nuevo_df])
  combined_hist['business_id'].drop_duplicates(inplace = True)
  
  # Guardamos los registros id's nuevos de business
  destination_bucket = 'carga_incremental'
  output_file_path = f'gs://{destination_bucket}/data_historica/Yelp.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_hist.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar el registro histórico de Yelp (new): {round(save_end_time - save_start_time, 4)} segundos")
  


  # TRANSFORMACIONES A PARTIR DE REVIEWS
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones

  # CREAR UN NUEVO DATAFRAME CON LA COLUMNA 'date'
  try:
    # Verificar si la columna 'date' está en formato datetime
    if pd.api.types.is_datetime64_any_dtype(review_final['date']):
      # Crear una nueva columna 'date_only' sin la hora como cadena de texto
      review_final['date_only'] = review_final['date'].dt.strftime('%Y-%m-%d')
    else:
      # Convertir la columna 'date' a tipo de dato DateTime
      review_final['date'] = pd.to_datetime(review_final['date'])
      # Crear una nueva columna 'date_only' sin la hora como cadena de texto
      review_final['date_only'] = review_final['date'].dt.strftime('%Y-%m-%d')

    # Obtener las fechas únicas de la columna 'date_only' como cadena de texto
    unique_dates = review_final['date_only'].unique()

  except Exception as e:
    print("Error:", e)

  # Extraer el archivo 'dates' limpio de Yelp para obtener el último id registrado
  bucket_name = 'data_limpia'
  input_file_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/dates.parquet')
  dates = pd.read_parquet(input_file_path)
  # Asegurar que los tipos de datos son correctos para evitar errores al concatenar o al guardar 
  dates['date'] = dates['date'].astype(str)
  dates['date'] = dates['date'].str.strip()
  # Obtener el máximo valor actual de date_id en el DataFrame existente
  max_date_id = dates['date_id'].max()

  # Crear un nuevo DataFrame usando la misma estructura del archivo 'dates', pero con las fechas almacenadas en unique_dates
  new_dates = pd.DataFrame({'date': unique_dates})
  new_dates = new_dates.sort_values(by='date')
  # Asegurar que los tipos de datos son correctas para evitar errores al concatenar o al guardar
  new_dates['date'] = new_dates['date'].astype(str)
  new_dates['date'] = new_dates['date'].str.strip()

  # Filtrar 'dates' y 'new_dates' para verificar si hay fechas nuevas
  fechas_nuevas = new_dates[~new_dates['date'].isin(dates['date'])]
  # Generar los nuevos id's para las fechas nuevas a partir de 'max_date_id'
  fechas_nuevas.loc[:, 'date_id'] = range(dates['date_id'].max()+1, max_date_id+1+len(fechas_nuevas))

  # Concatenar las fechas nuevas al DataFrame de fechas existente
  combined_dates = pd.concat([dates, fechas_nuevas])
  # Asegurar los tipos de datos antes de guardar en parquet
  combined_dates['date'] = combined_dates['date'].astype(str)
  combined_dates['date'] = combined_dates['date'].str.strip()
  combined_dates['date_id'] = combined_dates['date_id'].astype(int)

  # Guardar y actualizar combined_dates como archivo Parquet, para el bucket 'data_limpia'
  destination_bucket = 'data_limpia'
  dates_file_path = f'gs://{destination_bucket}/Yelp/dates.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_dates.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar dates: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de dates como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  dates_file_path = f'gs://{destination_bucket}/Yelp/dates.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  fechas_nuevas.to_parquet(dates_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar dates (new): {round(save_end_time - save_start_time, 4)} segundos")
  
  # Registro de tiempo después de las transformaciones a partir de review
  transform_end_time = time.time()
  print(f"Tiempo de transformación a partir de review: {round(transform_end_time - transform_start_time, 4)} segundos")


  # Asegurar que los tipos de datos en review para evitar conflictos
  review_final['date'] = review_final['date'].astype(str)
  review_final['date'] = review_final['date'].str.strip()
  # Merge_review entre los dataframes
  merged_review = review_final.merge(combined_dates, left_on='date', right_on='date', how='left')
  # Eliminar la columna 'date'
  merged_review.drop(columns=['date'], inplace=True)

  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()  
  print(f"Tiempo de transformación total de review: {round(transform_end_time - transform_start_time_r, 4)} segundos")

  # Concatenar con el archivo limpio de review 
  bucket_name = 'data_limpia'
  review_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/review.parquet')
  review_limpio = get_read_file(review_path)
  combined_review = pd.concat([review_limpio, merged_review])

  # Guardar el archivo 'review' transformado en el bucket
  destination_bucket = 'data_limpia'
  output_file_path = f'gs://{destination_bucket}/Yelp/review.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_review.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar review: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de review como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  output_file_path = f'gs://{destination_bucket}/Yelp/review.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  merged_review.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar review (new): {round(save_end_time - save_start_time, 4)} segundos")
  
  # Se retorna los dataframes con data nueva tanto de business como reviews, para ser usadas en las otras funciones 
  return business_final, merged_review


def transform_user(review, df):
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones

  # Eliminación de registros duplicados
  df.drop_duplicates(inplace=True)
  # Filtrando user con review
  user_filtrado = df[df["user_id"].isin(review["user_id"])]

  # Eliminamos columnas que no se usarán
  user_filtrado = user_filtrado.drop(columns=[ 'useful', 'funny', 'cool', 'elite', 'friends', 'fans', 'average_stars', 'compliment_hot', \
       'compliment_more', 'compliment_profile', 'compliment_cute', 'compliment_list', 'compliment_note', 'compliment_plain',\
       'compliment_cool', 'compliment_funny', 'compliment_writer', 'compliment_photos'])
  # Imputación de datos faltantes
  user_filtrado = user_filtrado.fillna('')

  # Concatenar con el archivo limpio de user
  bucket_name = 'data_limpia'
  user_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/user.parquet')
  user_limpio = get_read_file(user_path)
  combined_user = pd.concat([user_limpio, user_filtrado])
  combined_user.drop_duplicates(inplace=True)

  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()
  print(f"Tiempo de transformación de user: {round(transform_end_time - transform_start_time, 4)} segundos")

  # Guardar y actualizar el archivo user como archivo Parquet para el bucket 'data_limpia'
  destination_bucket = 'data_limpia'
  output_file_path = f'gs://{destination_bucket}/Yelp/user.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_user.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actualizar user: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardar los registros nuevos de user como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  output_file_path = f'gs://{destination_bucket}/Yelp/user.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  user_filtrado.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar user (new): {round(save_end_time - save_start_time, 4)} segundos")

  # Se retorna el Dataframe con data nueva de user, para ser usadas en las otras funciones 
  return user_filtrado


def transform_tip(business, user, df):
  transform_start_time = time.time()  # Registro de tiempo antes de las transformaciones

  # Eliminación de registros duplicados
  df.drop_duplicates(inplace=True)
  # Filtrado de tip para obtener los registros correspondientes a los rubros y estados con 'business'
  tip_filtradoBus = df[df["business_id"].isin(business["business_id"])]
  # Filtrado nuevamente pero con 'user_filtrado'
  tip_filtradoUser = tip_filtradoBus[tip_filtradoBus["user_id"].isin(user["user_id"])]
  # Eliminar duplicados surgidos de los filtros aplicados
  tip_filtradoUser = tip_filtradoUser.drop_duplicates(subset=['user_id', 'business_id', 'date'])
  # Eliminar columnas que no se usarán
  tip_filtradoUser = tip_filtradoUser.drop(columns=['compliment_count'])

  # Concatenar con el archivo limpio de tip
  bucket_name = 'data_limpia'
  tip_path = 'gs://{}/{}'.format(bucket_name, 'Yelp/tip.parquet')
  tip_limpio = get_read_file(tip_path)
  combined_tip = pd.concat([tip_limpio, tip_filtradoUser])
  combined_tip.drop_duplicates(inplace=True)

  # Registro de tiempo después de las transformaciones
  transform_end_time = time.time()
  print(f"Tiempo de transformación de tip: {round(transform_end_time - transform_start_time, 4)} segundos")

  # Guardamos y actualizamos tip como archivo Parquet para el bucket 'data_limpia'
  destination_bucket = 'data_limpia'
  output_file_path = f'gs://{destination_bucket}/Yelp/tip.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  combined_tip.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar y actaulizar tip: {round(save_end_time - save_start_time, 4)} segundos")
  # Guardamos los registros nuevos de tip como archivo Parquet para ser enviados a BigQuery
  destination_bucket = 'carga_incremental'
  output_file_path = f'gs://{destination_bucket}/Yelp/tip.parquet'
  save_start_time = time.time()  # Registro de tiempo antes de guardar el archivo parquet
  tip_filtradoUser.to_parquet(output_file_path)
  save_end_time = time.time()  # Registro de tiempo después de guardar el archivo parquet
  print(f"Tiempo para guardar tip (new): {round(save_end_time - save_start_time, 4)} segundos")

  return tip_filtradoUser


# Decorador para registrar una función como HTTP
@functions_framework.http
def timer_function(request):
  start_time = time.time()  # Registro de tiempo de inicio
  # Nombre del bucket donde serán extraídos los nuevos archivos en Cloud Storage
  bucket_name = 'data_extraccion'
  file_paths = [
        'gs://{}/{}'.format(bucket_name, 'entry_test/business.json'),
        'gs://{}/{}'.format(bucket_name, 'entry_test/user.parquet'),
        'gs://{}/{}'.format(bucket_name, 'entry_test/tip.json')]

  # Definir las variables fuera del bucle
  business = None
  review = None
  user = None
  tip = None

  # Bucle que lee todos los archivos y llama a las funciones para las transformaciones
  for file_path in file_paths:
    df = get_read_file(file_path)
    if df is None:
      print(f"Error al leer el archivo: {file_path}")
      break  # Detener el bucle si df es None

    # Verifica el nombre del archivo para llamar a su respectiva función 
    name = file_path.split('/')[-1].split('.')[-2]
    if name == 'business':
      business, review = transform_business_review(df)
    elif name == 'user':
      user = transform_user(review, df)
    elif name == 'tip':
      tip = transform_tip(business, user, df)

  end_time = time.time()  # Registro de tiempo de finalización
  total_time = end_time - start_time  # Cálculo del tiempo total de ejecución
  print(f"Tiempo total de ejecución: {total_time} segundos")
  return 'Procesamiento de archivos completado'  # Devolver un mensaje de éxito