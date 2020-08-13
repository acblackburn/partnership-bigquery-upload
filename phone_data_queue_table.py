import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

def p_0_calc(c,lambda_hour,mew,rho):
    #first_term = sum(((1/factorial(c-1))*((c*rho)**(c-1))) for c in range(1,c+1))
    #second_term = (1/factorial(c))*((rho*c)**c)*(1/(1-rho))
    first_term = sum(((1/factorial((c-1)))*((lambda_hour/mew)**(c-1))) for c in range(1,c+1))
    second_term = (1/factorial(c))*((lambda_hour/mew)**c)*((mew*c)/(mew*c - lambda_hour))
    p_0 = 1/(first_term + second_term)
    return p_0

def l_q_calc(c,lambda_hour,mew,p_0,rho):
    #factor = (1/factorial(c))*((lambda_hour/mew)**c)*(rho/(1 - rho)**2)
    factor = (1/factorial(c-1))*((lambda_hour/mew)**c)*((mew*lambda_hour)/(c*mew - lambda_hour)**2)
    return factor*p_0

def l_s_calc(l_q,lambda_hour,mew):
    return l_q + (lambda_hour/mew)

def l_w_calc(c,mew,lambda_hour):
    return ((c*mew)/(c*mew - lambda_hour))

def w_q_calc(l_q,lambda_hour):
    return l_q/lambda_hour

def w_s_calc(l_s,lambda_hour):
    return l_s/lambda_hour

def rho_calc(lambda_hour,c,mew):
    rho = lambda_hour/(c*mew)
    return rho

def factorial(n):
    fact = 1
    if n==0:
        fact = 1
    else:
        for i in range(1,n+1):
            fact = fact * i
    return fact

def queing_table(input_file):
    #read data in 
    df = pd.read_csv(input_file)

    #set capacity from 1-10
    capacity = np.arange(1,11)
    df['Start time'] = pd.to_datetime(df['Start time'], format="%Y-%m-%d %H:%M:%S")
    df['Duration'] = pd.to_timedelta(df['Duration'])
    no_days = len(df['Start time'].dt.normalize().value_counts())
    df = df.groupby(df['Call_ID'])

    #creating dictionaries for desired variables
    arrival_rate = {}
    waiting_time = {}
    service_time = {}
    for i in range(7,21):
        arrival_rate[i] = 0
        waiting_time[i] = 0
        service_time[i] = 0

    #fidning arrival rate
    for name, group in df:
        #choose which hour the call falls into (chosen the time of answer)
        hour =(group['Start time'][group['Event type'] == "Answer ACD"]).dt.hour.values
        
        #ignores callls that have no answer
        if len(hour) == 0:
            continue
        else:
            hour = hour[0]
        
        #calculates arrival rates
        arrival_rate[hour] += (1/no_days)
    
    #finding waiting and service time
    for name, group in df:
        #choose which hour the call falls into (chosen the time of answer)
        hour =(group['Start time'][group['Event type'] == "Answer ACD"]).dt.hour.values
        
        #ignores callls that have no answer
        if len(hour) == 0:
            continue
        else:
            hour = hour[0]
        
        #calculates wating time, taken as ringing and time in queue
        wating_time_sum = 0
        queue = group['Duration'][group['Event type'] == "In Queue"]
        ringing = group['Duration'][group['Event type'] == "Ringing"]
        waiting_time_sum = sum(queue.values) + sum(ringing.values)
        waiting_time[hour] += (((waiting_time_sum.astype('timedelta64[ns]')) / np.timedelta64(1, 'h')))/(no_days*arrival_rate[hour])
        
        #calculates time on call, answer ACD
        service_time[hour] += (sum((group['Duration'][group['Event type'] == "Answer ACD"]).values).astype('timedelta64[ns]') / np.timedelta64(1, 'h'))/(no_days*arrival_rate[hour])
    
    #create an empty list to be added with dictionary values for df
    rows_df = []
    #using calculations to find waiting times and utilisation rates
    for hour in range(7,21):
        for c in capacity:
            if service_time[hour] == 0 or arrival_rate[hour] == 0:
                rho = 0
                w_q = 0
                #creating timestamp from hour
                hour_timestamp = f'{str(hour)}:00:00'
                #creating dictionary values
                dict_add = {'hour':hour_timestamp, 'capacity':c, 'utilisation_rate': rho, 'idle_rate': rho, 'waiting_time': w_q, 'arrival_rate': 0, 'service_rate_hour': 0}
                rows_df.append(dict_add)
            else:
                lambda_hour = arrival_rate[hour]
                mew = 1/service_time[hour]
                rho = rho_calc(lambda_hour, c, mew)
                p_0  = p_0_calc(c,lambda_hour,mew,rho)
                l_q = l_q_calc(c,lambda_hour,mew,p_0,rho)
                w_q = w_q_calc(l_q,lambda_hour)
                #creating timestamp from hour
                hour_timestamp = f"{str(hour)}:00:00"
                #creating dictionary values
                dict_add = {'hour':hour_timestamp, 'capacity':c, 'utilisation_rate': rho, 'idle_rate': (1-rho), 'calc_waiting_time': (w_q*60), 
                            'avg_waiting_time':(waiting_time[hour]*60), 'arrival_rate': lambda_hour, 'service_rate_hour': mew}
                rows_df.append(dict_add)
    month = 'June'
    year = 2020
    df_queue = pd.DataFrame(rows_df)
    df_queue['date'] = datetime.strptime(f'01/{month}/{year}', '%d/%B/%Y')
    return df_queue

def phone__data_clean(input_file):
    #read data in 
    df = pd.read_csv(input_file)
    
    df['Call_Dropped'] = 0
    df = df.groupby('Call_ID')

    for name,group in df:
        if "IVR Enter" in group["Event type"].unique() and "Answer ACD" not in group["Event type"].unique():
                group['Call_Dropped'][group["Event type"] == "IVR Enter"] = 1

# df = queing_table('phone_data_clean.csv')
# print(df)
# df.to_csv("phone_queuing_table.csv", index=False)