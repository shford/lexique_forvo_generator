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
"""
import pandas as pd

# ==== Configuration ====
user_path = os.path.expanduser('~')
input_file_path = f'{user_path}/Documents/flashcard_project_new/Lexique383.csv'
output_file_path = f'{user_path}/Documents/flashcard_project_new/Lexique383 - Master.csv'
# ========================

# Read .csv with pandas
df = pd.read_csv(input_file_path, low_memory=False)

# Rename desired columns
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

# Remove unwanted columns
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

# Filter rows: ortho or lemme column string length > 2 (same as >= 3)
df = df[df["ortho"].astype(str).str.len() > 2]
df = df[df['lemme'].astype(str).str.len() > 2]

# Filter rows: cgram has ADJ, VER, ADV, ONO, PRE, CON, NOM, ADJ:ind where genre="m" & nombre='s'
desired_POS = ["adj", 'VER', 'ADV', 'ONO', 'PRE', 'CON', 'NOM', 'ADJ:ind']
df = df[df['cgram'].isin(desired_POS)]

# Misc removals.
df = df[df['lemme'].astype(str) != 'FALSE']
df = df[df['lemme'].astype(str) != 'TRUE']
df = df[df['lemme'].astype(str) != 'zzz']
df = df[df['lemme'].astype(str) != 'zzzz']
df = df[df['lemme'].astype(str) != 'o']
df = df[df['lemme'].astype(str) != 'team']
df = df[df['lemme'].astype(str) != '58e']

# Create a new ODS document
df.to_csv(output_file_path, index=False)
print(f"Wrote clean .csv file saved to: {output_file_path}")
