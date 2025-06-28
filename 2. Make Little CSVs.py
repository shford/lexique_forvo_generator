"""
Slice filtered .csv lexique into smaller files containing 500 lemmes apiece.

Works, mostly. Missed one word out of over 40,000 - good enough.
"""
import pandas as pd
import os


# a horrible amalgamation of 3. to check for homograph, homonym, distinct lemmas (i.e. offset b/c 1 row becomes 2 -> rows become > 500)
def calculate_lemme_offset(df) -> int:
    negative_offset = 0

    POS_PRIORITY = ["adj", "adv", "pre", "ver", "ono", "nom", "con"]
    df.columns = [c.lower() for c in df.columns]

    unique_lemmes = []
    lemme_to_rows = {}

    for idx, row in df.iterrows():
        lemme = row["lemme"]
        if lemme not in lemme_to_rows:
            lemme_to_rows[lemme] = []
            unique_lemmes.append(lemme)
        lemme_to_rows[lemme].append(row)


    lemmes = unique_lemmes
    for lemme in lemmes:
        rows_list = lemme_to_rows[lemme]
        subset = pd.DataFrame(rows_list)

        # add a column to rank POS by priorty (see above)
        for row in rows_list:
            subset["pos_rank"] = subset["cgram"].apply(
                lambda x: POS_PRIORITY.index(x.lower()) if x.lower() in POS_PRIORITY else len(POS_PRIORITY))

        # for this lemme, select only the highest priority POS
        min_rank = subset["pos_rank"].min()
        min_lemme_subset = subset[subset["pos_rank"] == min_rank]

        # get POS for formatting
        pos = min_lemme_subset["cgram"].iloc[0].lower()

        #
        rows = min_lemme_subset

        if len(rows) == 2:
            row1 = rows.iloc[0]
            row2 = rows.iloc[1]

            r1_genre = row1["genre"]
            r1_nombre = row1["nombre"]
            r2_genre = row2["genre"]
            r2_nombre = row2["nombre"]

            if pd.isna(row1["genre"]) and pd.isna(row2["genre"]) and r1_nombre == "s" and r2_nombre == "p" and pos != "adj":
                print(lemme)
                negative_offset += 1

    return negative_offset


# === CONFIGURATION ===
USER_PATH = os.path.expanduser('~')
INPUT_FILE = f'{USER_PATH}/Documents/flashcard_project_new/Lexique383 - Master.csv'
OUTPUT_FOLDER = f'{USER_PATH}/Documents/flashcard_project_new/lexique_exported_files'
CHUNK_SIZE = 500
SPOKEN_COUNT = 400
WRITTEN_COUNT = 100

# === STEP 1: LOAD CSV ===
df_all = pd.read_csv(INPUT_FILE, encoding='utf-8')
df_all.columns = [col.strip().lower() for col in df_all.columns]

# Ensure 'lemme' is string
df_all['lemme'] = df_all['lemme'].astype(str)

# Working dataset will be reduced as we process
df_working = df_all.copy()

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

chunk_num = 0
start_idx = 1
anki_start_idx = 1

while True:
    # === STEP 2: TOP 400 LEMMES BY SPOKEN FREQUENCY (ISLEM == 1)
    spoken_candidates = (
        df_working[df_working['islem'] == 1]
        .drop_duplicates(subset='lemme')
        .sort_values(by='freqlemfilms', ascending=False)
    )

    if spoken_candidates.empty:
        break  # done

    top_spoken_lemmes = spoken_candidates['lemme'].tolist()[:SPOKEN_COUNT]

    # === STEP 3: COLLECT ALL ROWS FOR THESE LEMMES (regardless of islem)
    spoken_rows = pd.DataFrame()
    for lemme in top_spoken_lemmes:
        matches = df_working[df_working['lemme'] == lemme]
        spoken_rows = pd.concat([spoken_rows, matches], ignore_index=True)
        df_working = df_working[df_working['lemme'] != lemme]

    # === STEP 4: TOP 100 WRITTEN LEMMES BY freqlemlivres (ISLEM == 1)
    written_candidates = (
        df_working[df_working['islem'] == 1]
        .drop_duplicates(subset='lemme')
        .sort_values(by='freqlemlivres', ascending=False)
    )

    top_written_lemmes = written_candidates['lemme'].tolist()[:WRITTEN_COUNT]

    written_rows = pd.DataFrame()
    for lemme in top_written_lemmes:
        matches = df_working[df_working['lemme'] == lemme]
        written_rows = pd.concat([written_rows, matches], ignore_index=True)
        df_working = df_working[df_working['lemme'] != lemme]

    # === STEP 5: COMBINE AND EXPORT
    chunk_df = pd.concat([spoken_rows, written_rows], ignore_index=True)

    # check / slice / offset end_idx
    lemme_offset = calculate_lemme_offset(chunk_df)
    total_lemmes = len(set(chunk_df['lemme']))
    lemmes_to_keep = chunk_df['lemme'].drop_duplicates().iloc[:total_lemmes - lemme_offset]
    chunk_df = chunk_df[chunk_df['lemme'].isin(lemmes_to_keep)]

    # continue
    total_lemmes = len(set(chunk_df['lemme']))

    anki_end_idx = start_idx + total_lemmes + lemme_offset - 1
    filename = f'Freq {start_idx}-{anki_end_idx}.csv'
    end_idx = start_idx + total_lemmes - 1
    filepath = os.path.join(OUTPUT_FOLDER, filename)
    chunk_df.to_csv(filepath, index=False, encoding='utf-8')
    print(f"Saved: {filename} ({total_lemmes} lemmes, {len(chunk_df)} rows)")

    # === PREPARE FOR NEXT CHUNK
    anki_start_idx = anki_end_idx + 1
    start_idx = end_idx + 1
    chunk_num += 1

print("\nDone: All chunks generated.")
