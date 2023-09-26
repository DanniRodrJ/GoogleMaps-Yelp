import streamlit as st
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import pandas as pd
import folium

# Descargar el lexicon (diccionario) necesario para NLTK
nltk.download('vader_lexicon')

# Crear una instancia del analizador de sentimientos de NLTK
sia = SentimentIntensityAnalyzer()


# # Título de la aplicación con una imagen encima
# image = 'imagenes\systech-logo.jpeg'  # Ruta de tu imagen
# st.image(image, use_column_width=True)  # Ajusta el ancho a la columna


# # Título de la aplicación con una imagen encima y alineada a la izquierda
# logo_image = 'imagenes\systech-logo.jpeg'  # Ruta de tu imagen
# st.image(logo_image, use_column_width=False, width=150)  # Establece el ancho de la imagen



# Título de la aplicación
st.title("Restaurant recommendation system")

# Entrada de texto del usuario para el análisis de sentimiento
texto_usuario = st.text_area("Enter your food preferences to get our best recommendations.")


# Cargar el archivo CSV desde la misma carpeta
file_path = "df_google.csv"

# Leer el archivo CSV en un DataFrame de pandas
df = pd.read_csv(file_path)

# Filtrar y eliminar filas con valores NaN en la columna "text"
df = df.dropna(subset=['text'])

# Calcular el análisis de sentimiento en la columna "text" del DataFrame (después de eliminar NaN)
df['Feeling'] = df['text'].apply(lambda x: sia.polarity_scores(str(x))['compound'])

# Recomendación basada en el análisis de sentimiento del texto del usuario
if texto_usuario:
    # Calcular el análisis de sentimiento para el texto del usuario
    sentimiento_usuario = sia.polarity_scores(texto_usuario)

    # Determinar el sentimiento general del texto del usuario
    if sentimiento_usuario['compound'] >= 0.05:
        resultado_sentimiento_usuario = "positive feeling"
    elif sentimiento_usuario['compound'] <= -0.05:
        resultado_sentimiento_usuario = "Negative feeling"
    else:
        resultado_sentimiento_usuario = "Neutral feeling"

    # Mostrar los resultados del análisis de sentimiento para el texto del usuario
    st.subheader("Sentiment Analysis for User Text:")
    st.write("Entered Text:", texto_usuario)
    st.write("Result:", resultado_sentimiento_usuario)
    st.write("Sentiment Score:", sentimiento_usuario['compound'])

    # Calcular la similitud de coseno entre el texto del usuario y los textos del DataFrame filtrados
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_matrix = tfidf_vectorizer.fit_transform(df['text'])
    cosine_similarities_usuario = linear_kernel(tfidf_matrix, tfidf_vectorizer.transform([texto_usuario]))
    df['User_Similarity'] = cosine_similarities_usuario

    # Ordenar por similitud y mostrar la recomendación con el "gmap_id"
    df_usuario = df.sort_values(by='User_Similarity', ascending=False).head(5)
    st.subheader("Recommendations:")
    st.write(df_usuario[['name', 'description']])
    #st.write(df_usuario[['name', 'description', 'Feeling', 'User_Similarity']])

    # # Mostrar los 5 primeros resultados en una tabla
    # st.subheader("Los 5 primeros resultados de la recomendación:")
    # st.write(df_usuario[['name', 'description', 'Sentimiento', 'Similitud_Usuario']].reset_index(drop=True))

    # Crear un mapa de Folium y agregar marcadores para las ubicaciones de los 5 primeros resultados
    st.subheader("Locations on the Map:")
    m = folium.Map(location=[df_usuario['latitude'].mean(), df_usuario['longitude'].mean()], zoom_start=10)

    for index, row in df_usuario.iterrows():
        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=row['name'] + ": " + row['description']
        ).add_to(m)

    # Mostrar el mapa en Streamlit usando st.components.v1.html
    st.components.v1.html(m._repr_html_(), width=700, height=500)

