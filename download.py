from asyncio import subprocess
import img2dataset
import pandas as pd
from glob import glob
from tqdm import tqdm
import tarfile
import shutil
import argparse
import sqlite3


# getting the start and end index for csv files
parser = argparse.ArgumentParser('reading the start and end index for csvs')
parser.add_argument('--start_index', type=int, default=0)
parser.add_argument('--end_index', type=int, default=10)
parser.add_argument('--csvs_dir', type=str, default='species_csv')
parser.add_argument('--save_csv_list', type=bool, default=1)
args = parser.parse_args()

print(args.start_index, args.end_index, args.csvs_dir, args.save_csv_list)

# path that the csv_list.csv file will be stored in
csv_list_dir = 'csv_list.csv'


def save_csv_list():
    """ saving the csv_list.csv file that contains the name of all CSVs"""
    
    if args.save_csv_list: 
        
        # listing all csv files in the csv directory
        csv_list = glob(f'{args.csvs_dir}/*.csv')
        
        # converting the list to pandas dataframe
        df = pd.DataFrame(csv_list, columns=['csv_list'])
        df['downloaded'] = False
        df.reset_index(inplace=True)
        
        # saving the dataframe as csv
        df.to_csv(csv_list_dir, index=False)
    
        # saving the csv as sql database
        # df.to_sql('csv_list_table', con=sqlite3.connect(f'sql_inaturalist_insects.db'), if_exists='replace')


def checking_conditions(number_of_csvs=0):
    """ checking the conditions for downloading the images """

    assert args.end_index < number_of_csvs, f'end index is greater than the number of csvs: {number_of_csvs}'
    assert args.start_index < args.end_index, 'start index can not be greater than end index'
    assert args.start_index >= 0, 'start index can not be negative'
    assert args.end_index >= 0  , 'end index can not be negative'
    
    
def updating_csv_list_table():
    """ updaing the csv_list csv file with the newly downloaded species
        Note: The file is being read again here to ensure that we are pulling the most recent version
        in case another instance has already modified it during the download process for this instance."""
    
    df_csv_list = pd.read_csv(csv_list_dir, index_col='index')
    df_csv_list.loc[args.start_index:args.end_index, 'downloaded'] = True
    df_csv_list.to_csv(csv_list_dir, index=False)     
      

def main():
    
    # reading the list of all csv files
    save_csv_list()
    
    df_csv_list = pd.read_csv(csv_list_dir, index_col='index')
    
    checking_conditions(number_of_csvs=df_csv_list.shape[0])


    # creating the sqlite connection
    # sqliteConnection = sqlite3.connect(f'sql_inaturalist_insects.db')
    
    # creating the tar file that will contain all downloades from this instance
    tar_name = f'{args.start_index}_{args.end_index}.tar.gz'
    tar = tarfile.open(tar_name, "w:gz")

    # looping through all csv files that fell into the range of start and end index
    for i in tqdm(df_csv_list.loc[args.start_index:args.end_index, 'csv_list']):
        
        # reading the csv file for specie corresponding to index i
        df = pd.read_csv(i)
        
        # the output_folder is the taxonomy hierachy of that specie. This code assumes it will be the same for all images inside this csv
        output_folder = df.loc[0,'ancestry']
        img2dataset.download(processes_count=16,
                            thread_count=32,
                            url_list=i,
                            output_folder=output_folder,
                            output_format='webdataset',
                            input_format='csv',
                            url_col='photo_url_large',
                            number_sample_per_shard=200000,
                            distributor='multiprocessing',
                            resize_mode='no')
        
        # adding the newly downloaded images of a purticular specie to tar file
        tar.add(output_folder)
        
        # upading the sql table
        # sqliteConnection.cursor.execute(f'UPDATE csv_list_table SET downloaded = 1 WHERE index = "{i}"')

    tar.close()
    
    updating_csv_list_table()
        
    # Closing teh sql connection
    # sqliteConnection.commit()
    # sqliteConnection.close()

    return tar_name
    

    
if __name__ == '__main__':
    tar_name = main()
        
    # ds_dir="/iplant/home/shared/soynomics/inaturalist/downloaded_data/"
    # subprocess.run(['iput', f' -f {tar_name} {ds_dir}'])