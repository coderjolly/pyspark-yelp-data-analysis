import pandas as pd
from collections import Counter
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

dataset = r'/Users/hsj/Downloads/yelp datset/d1.csv'

column_names = ['state', 'is_open', 'count']
df_final = pd.DataFrame(columns=column_names)


if rank == 0:
    print(f'\nUsing MPI with {size} workers\n')


    def distribute_rows(n_rows: int, n_processes):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        for _ in range(1, n_processes - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows

        reading_info.append([None, skip_rows])
        return reading_info


    def time_taken(start_time, end_time, size):
        total_time = f'{(end_time - start_time):.1f}'
        print(f'\n------------>  Time Taken by {size} workers : {total_time} seconds \n')



    slave_workers = size - 1
    tot_rows = 460402
    chunk_distribution = distribute_rows(n_rows=(tot_rows // slave_workers) + 1, n_processes=slave_workers)

    start_time = MPI.Wtime()  # Starting Time ------------------------------------------------
    for worker in range(1, size):
        chunk_to_process = worker - 1
        comm.send(chunk_distribution[chunk_to_process], dest=worker)

    # receive and aggregate results from slave
    results = []

    for worker in (range(1, size)):  # receive
        result = comm.recv(source=worker)
        df_final=pd.concat([df_final, result])
    df_grouped = df_final.groupby('state')['count'].sum().reset_index().sort_values('count', ascending=False)
    print(df_grouped)
    end_time = MPI.Wtime()  # Ending Time ------------------------------------------------
    time_taken(start_time, end_time, size)

elif rank > 0:

    def chunk_query(df):
        uc6 = df.loc[df['state'].str.len() == 2]
        uc6 = uc6.groupby(['state', 'is_open'])
        uc6 = uc6.size().reset_index(name='count')
        uc6 = uc6.loc[uc6['is_open'] == 1]
        return uc6


    chunk_to_process = comm.recv()
    columns = list(pd.read_csv(dataset, skiprows=0, nrows=0).columns)
    
    df = pd.read_csv(dataset, nrows=chunk_to_process[0], skiprows=chunk_to_process[1], names=columns)
    result = chunk_query(df)
    comm.send(result, dest=0)
