# Apache Spark
Aprendiendo a usar Apache Spark con ejemplos

Hay que instalar previamente varias cosas:
#### Descargar Apache Spark localmente

https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz [Link Text](https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz)


~~~
# Instalar Java si no lo tienes
sudo apt-get update
sudo apt-get install default-jdk

# Descargar e instalar Spark
cd /home/usuario/Documentos #reemplazar usuario por tu nombre de usuario
tar xvf spark-3.5.4-bin-hadoop3.tgz
sudo mv spark-3.5.4-bin-hadoop3 /opt/spark

~~~

#####Configurar las variables de entorno. Añade esto con vim o nano a tu ~/.bashrc o ~/.zshrc:
~~~
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
~~~
######Recarga tu shell:
~~~
source ~/.zshrc  # si usas zsh
# o
source ~/.bashrc  # si usas bash
~~~

Tambien

~~~
pip install pyspark
~~~

