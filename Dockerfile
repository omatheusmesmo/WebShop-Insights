# /mnt/fileshare/projects/python/WebShop/Dockerfile

FROM apache/airflow:2.9.3

# Mudar para o usuário AIRFLOW (UID é 50000 por padrão, mas a variável é mais segura)
# Ele tem permissão para instalar no seu ambiente virtual.
USER $AIRFLOW_UID

ADD requirements.txt .

# Instalar as dependências como usuário Airflow
RUN pip install --no-cache-dir -r requirements.txt

# Não precisamos da linha 'USER root' nem 'USER $AIRFLOW_UID' no final,
# pois o Airflow usa o ENTRYPOINT para configurar o usuário final.