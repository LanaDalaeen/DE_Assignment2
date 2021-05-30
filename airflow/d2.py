from numpy.lib.npyio import save
import datetime as dt
from datetime import timedelta
from airflow.operators.python_operators import PythonOperator
import pandas as pd 
from sqlalchemy import create_engine
import psycopg2
import matplotlib.pyplot as plt 
import matplotlib
from sklearn.preprocessing import MinMaxScaler

host="postgres" # use "localhost" if you access from outside the localnet docker-compose env 
database="testDB"
user="me"
password="1234"
port='5432'
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

def Get_DF_i(Day):
    DF_i=None
    try: 
        URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day=pd.read_csv(URL_Day)
        DF_day['Day']=Day
        cond=(DF_day.Country_Region=='United Kingdom')
        Selec_columns=['Day','Country_Region', 'Last_Update',
              'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
              'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
    except:
        pass



def import_data():
    Day='01-01-2021'
    URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
    DF_day=pd.read_csv(URL_Day)

    DF_day['Day']=Day
    cond=(DF_day.Country_Region=='United Kingdom')
    Selec_columns=['Day','Country_Region', 'Last_Update',
        'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
        'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
    DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)

    List_of_days=[]
    for year in range(2020,2022):
        for month in range(1,13):
            for day in range(1,32):
                month=int(month)
                if day <=9:
                    day=f'0{day}'

                if month <= 9 :
                    month=f'0{month}'
                List_of_days.append(f'{month}-{day}-{year}')

    DF_all=[]
    for Day in List_of_days:
        DF_all.append(Get_DF_i(Day))
        DF_UK=pd.concat(DF_all).reset_index(drop=True)
        # Create DateTime for Last_Update
        DF_UK['Last_Updat']=pd.to_datetime(DF_UK.Last_Update, infer_datetime_format=True)  
        DF_UK['Day']=pd.to_datetime(DF_UK.Day, infer_datetime_format=True)  

    DF_UK['Case_Fatality_Ratio']=DF_UK['Case_Fatality_Ratio'].astype(float)

    DF_all=[]
    for Day in List_of_days:
        DF_all.append(Get_DF_i(Day))

    DF_UK=pd.concat(DF_all).reset_index(drop=True)
    # Create DateTime for Last_Update
    DF_UK['Last_Updat']=pd.to_datetime(DF_UK.Last_Update, infer_datetime_format=True)  
    DF_UK['Day']=pd.to_datetime(DF_UK.Day, infer_datetime_format=True)  

    DF_UK['Case_Fatality_Ratio']=DF_UK['Case_Fatality_Ratio'].astype(float)
    
    DF_UK.to_sql('uk_scoring_report', engine, if_exists='replace',index=False)


def transformation():
    Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
    DF_UK_2=pd.read_sql('uk_scoring_report', engine)
    min_max_scaler = MinMaxScaler()
    DF_UK_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_UK_2[Selec_Columns]),columns=Selec_Columns)
    DF_UK_3.index=DF_UK_2.index
    DF_UK_3['Day']=DF_UK_2.Day
    DF_UK_3[Selec_Columns].plot(figsize=(20,10))
    plt.savefig('output/India_scoring_report.png')
    DF_UK_2.to_sql('output/uk_scoring_report_NotScaled.csv')


default_args = { 'owner' :'me',
                  'start_date': dt.datetime(2021,5,28),
                  'retries':1,
                  'retry_delay': dt.timedelta(minutes=5)}

dag=DAG(dag_id='dag', default_args=default_args)
step_1_task= PythonOperator(task_id='import_data', python_collable=import_data, dag=dag)
step_2_task= PythonOperator(task_id='transformation', python_collable=transformation, dag=dag)

step_1_task>>step_2_task


