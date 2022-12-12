# CLO

## Análisis de los vuelos en EE.UU

El código de este proyecto se centra en analizar las características de los vuelos y sus reservas, con el fin de que sirva a las aerolíneas a mejorar sus servicios a los clientes y, de este modo, mejorar su experiencia. Está compuesto por varios archivos .py, cada uno de los cuales analiza distintos parámetros que interesan a las aerolíneas:

- El archivo fecha.py analiza las fechas en las que se reservan los vuelos y las fechas en las que se vuela.

- El archivo is.py analiza si los vuelos son reembolsables, si hacen escalas o no, y si son de economía básica.

- El archivo aerolinea.py analiza las aerolíneas más habituales en las que se ha volado en EE.UU.

- El archivo asientos.py analiza cuántos asientos libres quedaban en los vuelos.

- El archivo startingAirports.py analiza el número de veces que se utiliza cada aeropuerto como origen.

- El archivo destinationAirports.py analiza el número de veces que se utiliza cada aeropuerto como destino.

- El archivo LAXDestinations.py analiza el número de veces que se utiliza cada aeropuerto como destino desde LAX, el aeropuerto más vuelos.

Además de los archivos descritos arriba, también se incluyen los archivos de test cases y un archivo con el link al dataset (el archivo no ha sido posible incluirlo porque excedía los límites de GitHub).


### Dataset

El dataset ha sido obtenido de la plataforma Kaggle:

https://www.kaggle.com/datasets/dilwong/flightprices


### Como ejecutar la aplicación

Todo el código del proyecto ha sido dividido entre distintos .py y almacenados en la carpeta 'code' del repositorio. Para ejecutarlos y obtener los csv's, se recomienda hacer el uso de la plataforma Google Cloud.

Pasos a seguir:

0. Como paso previo, debemos registrarnos y obtener créditos de cualquier forma en la plataforma, además de descargar el dataset del link proporcionado.

1. Crear un Bucket dentro de Cloud Storage de clase standart y cargar ahí el dataset, en una carpeta llamada 'input'.

2. Crear un clúster. Para ello abrir la consola cloud shell y escribir lo siguiente:
gcloud dataproc clusters create example-cluster --region europe-west6 --enable-component-gateway --master-boot-disk-size 50GB --worker-boot-disk-size 50GB
Además, asegurarse de que esta en estado 'En ejecución' en el apartado Clústeres de Dataproc.

3. Subir los archivos a ejecutar al explorador de la shell.

4. Escribir por consola BUCKET=gs://<BUCKET>, donde <BUCKET> sería el nombre del bucket creado anteriormente.

5. Ejecutar por consola el siguiente comando por cada archivo que se desea probar:
gcloud dataproc jobs submit pyspark --cluster example-cluster --region=europe-west6 <ARCHIVO>.py -- $BUCKET/input $BUCKET/<OUTPUT>
Donde <ARCHIVO> es el nombre del archivo que se desea ejecutar y <OUTPUT> es el nombre de la carpeta (sin crear) del bucket donde queremos que se guarde el csv resultante.

6. Para recuperar los resultados nos dirigiremos a la carpeta <OUTPUT> del bucket.

7. Detener la ejecución del cluster.
