FROM ubuntu:latest

User root 

ADD script.sh /script.sh
RUN chmod 755 /script.sh
RUN apt-get update
RUN apt-get install -y python3-pip

RUN pip3 install --upgrade pip==20.2.4

RUN export AIRFLOW_HOME=~/airflow

# RUN AIRFLOW_VERSION=
# RUN PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# # For example: 3.6
# CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.0.2/constraints-3.6.txt
RUN pip3 install apache-airflow==2.0.1 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.0.1/constraints-3.8.txt"

# initialize the database
RUN airflow db init

RUN airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org \
    --password 123456

# RUN airflow webserver -D --port 8080 


# CMD ["airflow", "scheduler"]

CMD ["/script.sh"]