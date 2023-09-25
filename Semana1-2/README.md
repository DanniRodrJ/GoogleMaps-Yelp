## 🗄️​Data Cruda

Fue proporcionada por el cliente a través de Google Drive y que a partir de ella usando Google Colab realizamos el análisis exploratorio preliminar.

## 👀ETL-EDA Premilinar

A través de esta exploración se establecieron los criterios necesarios para desarrollar el proyecto. Por un lado definimos las columnas importantes y que pudiesen ser de utilidad, mientras que por otro lado se realizaron algunas transformaciones para poder definir los rubros clave. Los cuales fueron los siguientes:

```Python
    target_categories = [
    'restaurant', 'coffee', 'rice', 'paan', 'ice cream',
    'tortilla', 'tofu', 'pie', 'soup',
    'cheese', 'cupcake', 'pasta', 'cookie', 'chocolate',
    'frozen yogurt', 'salad', 'cake', 'donut',
    'sandwich', 'chicken', 'pizza', 'burguer', 'hot dog']
```

## 🧩​Stack Tecnológico

En un principio se tenía previsto el siguiente Stack basado en las tecnologías de Big Data en un entorno local.

![stack_anterior](/Imagenes/Stack_anterior.png)

Donde el almacenamiento centralizado iba a ocurrir aplicando tecnologías de Hadoop corriendo a través de Docker, mientras que el Data Warehouse iba a estar conformado por Hadoop Hive junto con la interfaz gráfica de Hue.

Sin embargo, al final se optó por migrar a la nube de Google Cloud Platform. Principalmente este cambio se debió a que podíamos simplificar la gestión y el mantenimiento de la infraestructura lo que nos permitió concentrarnos en el desarrollo del proyecto.

Utilizando finalmente el siguiente Stack, basado en principalmente en Google Cloud Platform (GCP)

![stack_anterior](/Imagenes/Stack_Tecnologico.png)

## 🛠️​Metodología Propuesta

El diagrama de Gantt fue utilizado en la realización del proyecto para facilitar la planificación y seguimiento de las actividades. Proporcionó una representación visual clara de las tareas, su secuencia y duración en el tiempo. Esto permitió una mejor organización del trabajo, asignación de recursos y coordinación entre los miembros del equipo. Además, el diagrama de Gantt ayudó a identificar hitos y tareas críticas, lo que permitió priorizar y enfocar los esfuerzos en los aspectos clave del proyecto.