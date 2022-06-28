# Algoritmo Count Distribution Apriori

## Istruzioni per eseguire il codice su Google Cloud Platform

1. Configura un nuovo progetto [Google Cloud](https://cloud.google.com/dataproc/docs/guides/setup-project]
2. Crea un bucket [Cloud Storage](https://cloud.google.com/storage/docs/creating-buckets) e caricaci:
  - il file JAR del codice
  - i dataset
3. Crea un [cluster Dataproc](https://cloud.google.com/dataproc/docs/guides/create-cluster) su Compute Engine
  - assicurati abbia come immagine '2.0 (Debian 10, Hadoop 3.2, Spark 3.1)'
  - seleziona quanti e che tipo di nodi avrà il cluster
4. Vai nella pagina del cluster appena creato ed invia un nuovo job:
  - nella sezione *Job type* seleziona Spark
  - nella sezione *Main class or jar* scrivi l'indirizzo al file JAR caricato sul bucket (Es: *gs://my_bucket_name/myFile.jar*)
  - nella sezione *Argomenti* inserisci tutti gli argomenti che il codice prende in input



