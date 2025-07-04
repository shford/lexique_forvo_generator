"""
@Instructions:
  1. Export:
    In LibreOffice, 'Save As' Lexique383.xlsb to Lexique383.csv (comma field separated, UTF-8) in desired directory.
  2. Set paths:
    Fill in desired input and output file paths.
  3. Adjust memory:
    a. Add memory to IDE. I gave my IDE 2 GiB. You could probably get away with less.
    b. If having issues maybe try: read_csv(..., low_memory=false) -> read_csv(..., low_memory=true)

@Purpose: Filter lexique lemmes so that:
 1. Rename necessary column names.
 2. Remove unwanted columns.
 3. Filter where lemme and ortho columns are both > 2 chars.
 4. Filter where cgram equals whitelisted values.
 5. Remove where dirty one off data.
 6. Save to CSV.

@Note: This is probably going to be very slow. On my machine it took ~13 minutes to run.
        But it only needs to run once. And I'm attaching the output so you don't need to run it at all.

Todo:
    - Optimize group_dfs_by_lemme().. 54% of runtime
    - Optimize filter_df_for_highest_pos().. 45% of runtime excluding group_dfs_by_lemme()
"""
import warnings

import pandas as pd
import os
import gc

# ==== Configuration ====
user_path = os.path.expanduser('~')
input_file_path = f'{user_path}/Documents/flashcard_project_new/Lexique383.csv'
output_file_path = f'{user_path}/Documents/flashcard_project_new/Lexique383 - Filtered.csv'
# ========================

# Globals
desired_POS = ['adj', 'ver', 'adv', 'ono', 'pre', 'con', 'nom', 'adj:ind']

def main():
    # read .csv with pandas
    df = pd.read_csv(input_file_path, low_memory=False)

    # rename desired column headers
    df.rename(columns={'1_ortho': "ortho",
                       '3_lemme': 'lemme',
                       '4_cgram': 'cgram',
                       '5_genre': "genre",
                       '6_nombre': "nombre",
                       '7_freqlemfilms2': 'freqlemfilms',
                       '8_freqlemlivres': 'freqlemlivres',
                       '14_islem': 'islem',
                       '28_orthosyll': 'orthosyll',
                       '29_cgramortho': 'cgramortho',
                       }, inplace=True)

    # remove unwanted columns
    df = df.drop(columns=['2_phon',
                          '9_freqfilms2',
                          '10_freqlivres',
                          '11_infover',
                          '12_nbhomogr',
                          '13_nbhomoph',
                          '15_nblettres',
                          '16_nbphons',
                          '17_cvcv',
                          '18_p_cvcv',
                          '19_voisorth',
                          '20_voisphon',
                          '21_puorth',
                          '22_puphon',
                          '23_syll',
                          '24_nbsyll',
                          '25_cv-cv',
                          '26_orthrenv',
                          '27_phonrenv',
                          '30_deflem',
                          '31_defobs',
                          '32_old20',
                          '33_pld20',
                          '34_morphoder',
                          '35_nbmorph'])

    # filter rows: ortho or lemme column string length > 2 (same as >= 3)
    df = df[df["ortho"].astype(str).str.len() > 2]
    df = df[df['lemme'].astype(str).str.len() > 2]

    # misc removals of known bogus
    df = df[df['lemme'].astype(str) != 'FALSE']
    df = df[df['lemme'].astype(str) != 'TRUE']
    df = df[df['lemme'].astype(str) != 'zzz']
    df = df[df['lemme'].astype(str) != 'zzzz']
    df = df[df['lemme'].astype(str) != 'o']
    df = df[df['lemme'].astype(str) != 'team']
    df = df[df['lemme'].astype(str) != '58e']

    # rename "cgram" column to .lower()
    df['cgram'] = df['cgram'].str.lower()

    # filter rows for cgram equals desired_POS
    df = df[df['cgram'].isin(desired_POS)]

    # filter for highest priority POS
    df = filter_df_for_highest_pos(df)

    # create a new ODS document
    df.to_csv(output_file_path, index=False, encoding='utf-8')
    print(f'Wrote clean .csv file saved to: {output_file_path}')


def filter_df_for_highest_pos(df) -> pd.DataFrame:
    # add column with POS rank
    df['pos_rank'] = df['cgram'].apply(lambda pos: desired_POS.index(pos) if pos in desired_POS else len(desired_POS))

    # get lemmes and their corresponding rows
    lemmes, lemme_to_df_lookup = group_dfs_by_lemme(df)

    # free old df - we'll be reconstructing it from scratch
    del df
    gc.collect()

    # populate df with only highest priority POS for each lemme
    df = pd.DataFrame()
    for l in lemmes:
        # get all rows associated with this lemme
        lemme_df = lemme_to_df_lookup[l]

        # filter
        min_rank = lemme_df['pos_rank'].min()
        lemme_df = lemme_df[lemme_df['pos_rank'] == min_rank]

        # append filtered rows to df
        df = pd.concat([df, lemme_df], ignore_index=True)

    # remove unneeded POS rank
    df = df.drop(columns=['pos_rank'])

    return df


def group_dfs_by_lemme(df) -> (list, dict):
    """
    Create a list of unique lexique lemmes.
    Create a dict of dataframes belonging to each lemme.
    """
    df_columns = df.columns

    unique_lemmes = []
    lemme_to_df_lookup = {}

    lemme_df = None
    for idx, row in df.iterrows():
        lemme = row['lemme']
        if lemme not in lemme_to_df_lookup:
            # add unique lemme to list
            unique_lemmes.append(lemme)

            # init dataframe for lemme
            lemme_to_df_lookup[lemme]: pd.DataFrame = pd.DataFrame(columns=df_columns)

        # update lemme's dataframe
        df_row = pd.DataFrame([row]) # prevent pandas from crushing our Series into a new column...
        lemme_to_df_lookup[lemme] = pd.concat([lemme_to_df_lookup[lemme], df_row], ignore_index=True)

    return unique_lemmes, lemme_to_df_lookup


if __name__ == '__main__':
    # ignore pandas 'FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.'
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=FutureWarning)
        main()
