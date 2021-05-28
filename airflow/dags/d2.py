from numpy.lib.npyio import save


try:
    from faker import Faker
except:
   !pip install faker 
   from faker import Faker
    
try:
    import psycopg2 
except:
    !pip install psycopg2-binary 
    import psycopg2
    
try:
    from sqlalchemy import create_engine
except:
    !pip install sqlalchemy
    from sqlalchemy import create_engine
    
    
try:
    import pandas as pd 
except:
    !pip install pandas
    import pandas as pd 
     
try:
    import matplotlib 
except:
    !pip install matplotlib
    import matplotlib

try:
    import sklearn 
except:
    !pip install sklearn
    import sklearn

import datetime as dt
from datetime import timedelta
from airflow.operators.python_operators import PythonOperator

def export_data():
 import pandas as pd 
 Day='01-01-2021'
 URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
 DF_day=pd.read_csv(URL_Day)
 DF_day['Day']=Day
 cond=(DF_day.Country_Region=='United Kingdom')
 Selec_columns=['Day','Country_Region', 'Last_Update',
       'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
       'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
 DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
 DF_i
 DF_all=[]
 for Day in List_of_days:
    DF_all.append(Get_DF_i(Day))
 DF_UK=pd.concat(DF_all).reset_index(drop=True)
 # Create DateTime for Last_Update
 DF_UK['Last_Updat']=pd.to_datetime(DF_UK.Last_Update, infer_datetime_format=True)  
 DF_UK['Day']=pd.to_datetime(DF_UK.Day, infer_datetime_format=True)  

 DF_UK['Case_Fatality_Ratio']=DF_UK['Case_Fatality_Ratio'].astype(float)

def transformation():
 Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
 DF_UK_2=DF_UK[Selec_Columns]
 DF_UK_2
 from sklearn.preprocessing import MinMaxScaler
 min_max_scaler = MinMaxScaler()
 DF_UK_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_UK_2[Selec_Columns]),columns=Selec_Columns)
 DF_UK_3.index=DF_UK_2.index
 DF_UK_3['Day']=DF_UK.Day

def load_scoring():
 DF_UK_3[Selec_Columns].plot(figsize=(20,10))
 plt.savefig('output/UK_scoring_report.png')
 DF_UK_3.to_csv('output/UK_scoring_report.csv')
 DF_UK_2.to_csv('output/UK_scoring_report_NotScaled.csv')

def save_to_table():
 from sqlalchemy import create_engine
 import psycopg2
 host="postgres" # use "localhost" if you access from outside the localnet docker-compose env 
 database="testDB"
 user="me"
 password="1234"
 port='5432'
 engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
 scores_extracted=pd.read_sql(f"SELECT * FROM UK_scoring_report=" , engine);
 scores_not_scaled_extracted=pd.read_sql(f"SELECT * FROM UK_scoring_notscaled_report" , engine);

default_args = { 'owner' :'me',
                  'start_date': dt.datetime(2021,5,28),
                  'retries':1,
                  'retry_delay': dt.timedelta(minutes=5)}

dag=DAG(dag_id='dag', default_args=default_args)
step_1_task= PythonOperator(task_id='export_data', python_collable=export_data, dag=dag)
step_2_task= PythonOperator(task_id='transformation', python_collable=transformation, dag=dag)
step_3_task= PythonOperator(task_id='load_scoring', python_collable=load_scoring, dag=dag)
step_4_task= PythonOperator(task_id='save_to_table', python_collable=save_to_table, dag=dag)
step_1_task>>step_2_task>>step_3_task>>step_4_task


