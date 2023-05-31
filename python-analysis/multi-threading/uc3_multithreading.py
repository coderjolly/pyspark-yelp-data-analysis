from threading import Thread
import time
import pandas as pd

dataset = r'/Users/hsj/Downloads/yelp datset/d1.csv'
column_names = ['state', 'is_open', 'count']
df_final = pd.DataFrame(columns=column_names)
num_of_threads=0

def chunk_query(df):
    uc3 = df.loc[df['state'].str.len() == 2]
    uc3 = uc3.groupby(['state', 'is_open'])
    uc3 = uc3.size().reset_index(name='count')
    uc3 = uc3.loc[uc3['is_open'] == 1]
    return uc3


def task(skip, read):
    if skip==0:
        skip=1
    columns=list(pd.read_csv(dataset, skiprows=0, nrows=0).columns)
    df = pd.read_csv(dataset, skiprows=skip, nrows=read,names=columns)
    result = chunk_query(df)
    global df_final 
    df_final= pd.concat([df_final, result])
    

def Input_Thread_Count():
    global num_of_threads
    num_of_threads = int(input("Enter the number of threads : "))


def Thread_function():
    thread_handle = []
    tot_rows = 460402
    to_read = round((tot_rows / num_of_threads)) + 1

    for j in range(0, num_of_threads):
        t = Thread(target=task, args=((to_read * j), to_read))
        thread_handle.append(t)
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()
        

def time_taken(start_time,end_time,num_of_threads):
    total_time= f'{(end_time - start_time):.1f}'
    print(f'\n------------>  Time Taken by {num_of_threads} threads : {total_time} seconds\n')

if __name__ == '__main__':
    start_time = time.time()
    Input_Thread_Count()
    Thread_function()
    df_grouped = df_final.groupby('state')['count'].sum().reset_index().sort_values('count', ascending=False)
    print(df_grouped)
    end_time = time.time()
    time_taken(start_time,end_time,num_of_threads)

