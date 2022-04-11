from asyncio import subprocess
import img2dataset
import pandas as pd
from glob import glob
from tqdm import tqdm
import tarfile
import argparse
import os
import shutil

""" Example: python3 download.py --start_index 0 --end_index 10 --data_dir ../data/ --force_rewrite_csv_list 0 """


# getting the start and end index for csv files
parser = argparse.ArgumentParser('reading the start and end index for csvs')
parser.add_argument('--start_index', type=int, default=95300)
parser.add_argument('--end_index', type=int, default=95398)
parser.add_argument('--data_dir', type=str, default='data')
parser.add_argument('--force_rewrite_csv_list', type=bool, default=0)
args = parser.parse_args()

# path that the csv_list.csv file will be stored in
csv_list_dir = f'{args.data_dir}/csv_list.csv'
# csv_list_dir = '/home/u29/mohammadsmajdi/projects/inaturalist/data/csv_list.csv'


def save_csv_list():
    """ saving the csv_list.csv file that contains the name of all CSVs"""
    
    if args.force_rewrite_csv_list or ( not os.path.exists(csv_list_dir) ): 
        
        # listing all csv files in the csv directory
        csv_list = glob(f'{args.data_dir}/species_csv/*.csv')
        
        # converting the list to pandas dataframe
        df = pd.DataFrame(csv_list, columns=['csv_list'])
        df.csv_list = df.csv_list.map(lambda x: x.replace(f'{args.data_dir}/species_csv/',''))
        df['downloaded'] = False
        
        # saving the dataframe as csv
        df.to_csv(csv_list_dir, index=True)
       
 
def update_csv_list(index_list: list):
    """ updaing the csv_list csv file with the newly downloaded species
        Note: The file is being read again here to ensure that we are pulling the most recent version
        in case another instance has already modified it during the download process for this instance."""
        
    df_csv_list = pd.read_csv(csv_list_dir, index_col=0)
    df_csv_list.loc[index_list, 'downloaded'] = True
    df_csv_list.to_csv(csv_list_dir, index=True)  


def checking_conditions(number_of_csvs=0):
    """ checking the conditions for downloading the images """

    assert args.end_index < number_of_csvs, f'end index is greater than the number of csvs: {number_of_csvs}'
    assert args.start_index < args.end_index, 'start index can not be greater than end index'
    assert args.start_index >= 0, 'start index can not be negative'
    assert args.end_index >= 0  , 'end index can not be negative'
    
    
   
      

def main(start_index=0, end_index=10, data_dir='', force_rewrite_csv_list=False):
    
    # reading the list of all csv files
    save_csv_list()
    
    df_csv_list = pd.read_csv(csv_list_dir, index_col=0)
    
    checking_conditions(number_of_csvs=df_csv_list.shape[0])
    
    # creating the tar file that will contain all downloades from this instance
    tar_name = f'{args.start_index}_{args.end_index}.tar.gz'
    tar = tarfile.open(f'{os.path.abspath(args.data_dir)}/{tar_name}', "w:gz")

    # looping through all csv files that fell into the range of start and end index
    index_list = list(range(args.start_index,args.end_index+1))
    
    for i in tqdm(df_csv_list.loc[index_list, 'csv_list']):
              
        try:
            # reading the csv belonging to one specie corresponding to index i of csv_list.csv file
            csv_dir = f'{args.data_dir}/species_csv/{i}'
            df = pd.read_csv(csv_dir)
            
            # the output_folder is the taxonomy hierachy of that specie. This code assumes it will be the same for all images inside this csv
            output_folder = f"{args.data_dir}/{df.loc[0,'ancestry']}"
            
            img2dataset.download(processes_count=16,
                                thread_count=32,
                                url_list=csv_dir,
                                output_folder=output_folder,
                                output_format='webdataset',
                                input_format='csv',
                                url_col='photo_url_large',
                                number_sample_per_shard=200000,
                                distributor='multiprocessing',
                                resize_mode='no')
            
            # adding the newly downloaded images of a purticular specie to tar file
            tar.add(os.path.abspath(output_folder), arcname=df.loc[0,'ancestry'])
            
            # removing the previously not-compressed downloaded images
            parent_dir = df.loc[0,'ancestry'].split('/')[0]
            shutil.rmtree(f'{args.data_dir}/{parent_dir}')
        
        except:
            index_list.pop(i)
        
    tar.close()
    
    update_csv_list(index_list)
    
    return tar_name

if __name__ == '__main__':
    tar_name = main(start_index=args.start_index, end_index=args.end_index, data_dir=args.data_dir, force_rewrite_csv_list=args.force_rewrite_csv_list)
