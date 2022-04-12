import pandas as pd
import dask.dataframe as dd
import os
import pandas as pd
from glob import glob
from tqdm import tqdm
import tarfile
import shutil
import argparse

""" Example: python3 download.py --start_index 0 --end_index 10 --data_dir ../data/ --force_rewrite_csv_list 0 """


# getting the start and end index for csv files
parser = argparse.ArgumentParser('reading the start and end index for csvs')
parser.add_argument('--dir_taxa', type=str, default='taxa.csv')
parser.add_argument('--dir_taxa_insecta', type=str, default='data/taxa_insecta.csv')
parser.add_argument('--dir_data', type=str, default='data')
parser.add_argument('--force_rewrite_taxa_insecta', type=bool, default=0)
# parser.add_argument('--untar_shards', type=bool, default=0)
# parser.add_argument('--restore_photo_id', type=bool, default=0)
args = parser.parse_args()


def rename_taxon_id_to_name(dir_data='dir_data', df=pd.DataFrame(), level=0):
    """ renaming taxon_id to taxon_name for all folders (not images) 
        EXAMPLE: python renaming_taxon_id_to_name.py --dir_taxa ../data/full_data_csv_lists/taxa.csv --dir_taxa_insecta ../data/full_data_csv_lists/taxa_insecta.csv --dir_data ../data/uncompressed/
    """
    
    # getting all subfolders inside the directory
    dir_data, subfolders, subfiles = next(os.walk(dir_data))
    print('level: ', level)
    
    for subfl in subfolders:
        
        # finding the mapping for taxon id to name
        taxon_name = df.loc[df.taxon_id == subfl, 'name'].values[0]
        
        # renmaing the folder
        os.rename(f'{dir_data}/{subfl}', f'{dir_data}/{taxon_name}')
        
        # contiuing the process for subfolders
        rename_taxon_id_to_name(f'{dir_data}/{taxon_name}', df, level+1)


def mapping_taxon_id_to_name(dir_taxa='/taxa.csv', dir_taxa_insecta='/taxa_insecta.csv', force_rewrite_taxa_insecta=False, dir_data='/data'):
    
    if not os.path.exists(dir_taxa_insecta) or force_rewrite_taxa_insecta:      
        
        # read taxa.csv file
        # dir = '/Users/personal-macbook/Documents/projects/inaturalist/data/full_data_csv_lists'      
        df_taxa = pd.read_csv(dir_taxa, sep='	', dtype=str)


        # Extracting the ancestors taxon_id map for insecta: 48460/1/47120/372739/47158
        ancestries = '48460/1/47120/372739/47158'.split('/')
        df_taxa_insecta_ancestors = df_taxa.loc[df_taxa.taxon_id.isin( ancestries)]

        # Extracting the rows under insecta
        df_taxa[df_taxa.isna()] = '-'
        df_taxa_insecta = df_taxa.loc[df_taxa.ancestry.str.contains('48460/1/47120/372739/47158')]


        # Merging the two tables
        df = pd.concat( (df_taxa_insecta_ancestors, df_taxa_insecta), axis=0)
        df.to_csv(dir_taxa_insecta, index=True)
        
    else:
        df = pd.read_csv(dir_taxa_insecta)
        
        
        
    # dir_data_main = '/Users/personal-macbook/Documents/projects/inaturalist/data/uncompressed'
    rename_taxon_id_to_name(dir_data=dir_data, df=df, level=0)



        
        



if __name__ == '__main__':
    mapping_taxon_id_to_name(**args.__dict__)
    