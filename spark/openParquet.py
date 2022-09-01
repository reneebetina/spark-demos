import os
import sys
import pyarrow.parquet as pq
import datetime

def read_parquet_file(folder, dt_partition, input_filename):
    my_input_full_path = os.path.abspath('input_data/' + folder + '/' + dt_partition + '/' + input_filename)
    print('Opening: ', my_input_full_path)

    print(">>> READING PARQUET FILE")
    df = pq.read_table(source=os.path.abspath(my_input_full_path)).to_pandas()
    print("Rows: " + str(len(df)))
    return df

def read_parquet_directory(loc):
    my_input_full_path = os.path.abspath(loc)
    print('Opening: ', my_input_full_path)

    fileList = []
    for f in next(os.walk(my_input_full_path)):
        for file in f:
            if ".parquet" in file:
                #fileList.append(os.path.join(my_input_full_path, file))
                fileList.append(file)

    #print(">>> PARQUET FILES DETECTED: ")
    #for file in fileList:
        #print(file)

    print(">>> READING PARQUET FILES")
    df = pq.read_table(source=os.path.abspath(my_input_full_path)).to_pandas()
    print("Rows: " + str(len(df)))
    return df

def write_output(df,output_path,filetype,singleMode=False):
    # CSV file maximum rows: 1,048,576
    # TXT max rows: 2,147,483,647
    # if you need to check a larger set - run on SPARK SHELL APPROACH
    # declare output details
    my_output_folder = output_path
    timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    my_output_filetype = filetype

    if singleMode==True:
        my_output_filename = 'extracted_data_as_of_' + str(timestamp) + my_output_filetype
    else:
        my_output_filename = 'full_data_as_of_' + str(timestamp) + my_output_filetype

    my_output_path = my_output_folder + my_output_filename

    # write data to csv or txt file
    if len(df) < 1000000000:
        df.to_csv(my_output_path)
        print(">>> EXPORT COMPLETE :", my_output_path)
    else:
        print("*************** EXPORT ABORTED: FILE TOO LARGE ***************")

if __name__ == "__main__":
    print("*************** READ SINGLE PARQUET ***************")
    my_dataframe_1 = read_parquet_file('all_types', '2022_08_31', 'alltypes_plain.parquet')
    write_output(my_dataframe_1, 'output_parquet/all_types/2022_08_31/', '.txt', singleMode=True)

    print("\n\n*************** READ SPECIFIC DIRECTORY (1 partition)***************")
    my_dataframe_2 = read_parquet_directory('input_data/all_types/2022_09_01')
    write_output(my_dataframe_2, 'output_parquet/all_types/2022_09_01/', '.csv')

    print("\n\n*************** READ FULL DIRECTORY (multiple partitions) ***************")
    my_dataframe_3 = read_parquet_directory('input_data/')
    write_output(my_dataframe_3, 'output_parquet/', '.txt')
