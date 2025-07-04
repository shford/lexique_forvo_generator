"""

Note 1: The formatting for adjectives and nouns is surprisingly inconvenient to decouple.
        It would require a lot of duplicate code so in format_noun_declension_adj() we actually
        may format_noun_declension_nom(). The adjective does not magically become a noun. I just
        didn't want to abstract out the rows 2 & 3 special cases (thanks French) and corrective logic.
        Likewise if you see a ["cgram"] == 'adj' inside the _nom() then now you know it's just there
        for this. Sorry eh.

Note 2: The Lexique 3.83 excel was already sorted such that nombre 's' came prior to "p".
        I doubt that'll change but if this breaks for future versions then it be worth adding
        a quick sort (not actually quicksort, jeez) to do that within the POS for each lemme.

"""
import os
import re

import numpy as np
import pandas as pd

# TODO
#   - fix naming discrepancy
#   - probably should be merged into one giant script

# === Configuration variables ===
USER_PATH = os.path.expanduser('~')
INPUT_CSV = f'{USER_PATH}/Documents/flashcard_project_new/lexique_exported_files/Freq 1 - 500.csv'
OUTPUT_DIR = f'{USER_PATH}/Documents/flashcard_project_new/anki_lexique_imports'
OUTPUT_PREFIX = 'anki_deck_'
CHUNK_SIZE = 500

# === End Config ===

# POS priority for sorting and filtering
POS_PRIORITY = ['adj', 'adv', 'pre', 'ver', 'ono', 'nom', 'con']

# Special lemme sets for formatting exceptions
HARD_CODED_BOLD = {'quelques', 'quelque'}
HARD_CODED_ADJ_4_ROWS = {
    'tout', 'toute', 'tous', 'toutes',
    'aucun', 'aucune', 'aucuns', 'aucunes',
}
SPECIAL_LEMME_FOIS = 'fois'


def main():
    formatting_exception_count = 0

    # load pandas
    df = pd.read_csv(INPUT_CSV)

    # calculate starting frequency index from filename
    freq_start = parse_start_frequency(INPUT_CSV)

    # ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # create a set of {"lemme1":[row_A, row_B], "lemme2":[row_X,...]} & ["lemme1", "lemme2", ...]
    lemmes, lemme_to_rows = group_rows_by_lemme(df)

    # process lemme in chunks of CHUNK_SIZE
    for chunk_idx in range(0, len(lemmes), CHUNK_SIZE):
        export_rows = []
        lemme_chunk = lemmes[chunk_idx : chunk_idx + CHUNK_SIZE]

        for lemme in lemme_chunk:
            lemme_df = pd.DataFrame(lemme_to_rows[lemme])
            pos = lemme_df['cgram'].iloc[0]

            # format 'Noun Declension' field
            noun_decl = format_noun_declension(lemme, lemme_df, pos)

            # if formatting fails, print all rows for this lemme and POS with nombre and ortho for debug
            if noun_decl is None:
                formatting_exception_count += 1
                print(f"Unhandled case for {lemme}, {pos}")
                for _, r in lemme_df.iterrows():
                    ortho_val = r.get('ortho', 'NaN')
                    genre_val = r.get('genre', 'NaN')
                    nombre_val = r.get('nombre', 'NaN')
                    print(f"\tortho: {ortho_val}, genre: {genre_val}, nombre: {nombre_val}")

                # default leave field blank - easier to find and fix by
                noun_decl = ''

            # copy orthosyll column to pronunciation column - use matching lemme orthosyll if available
            pronun_row = lemme_df[lemme_df['ortho'] == lemme]
            if not pronun_row.empty:
                pronun = pronun_row['orthosyll'].iloc[0]
            else:
                pronun = lemme_df['orthosyll'].iloc[0]

            # prepare row for export
            if noun_decl is not False:
                if isinstance(noun_decl, list):
                    noun_decls = noun_decl # it's hideous, i know, i'm sorry
                    for noun_decl in noun_decls:
                        # apply contraction rule to noun_decl
                        noun_decl = apply_contraction(noun_decl)

                        # append
                        export_rows.append({
                            'Lemme': lemme,
                            'Noun Declension': noun_decl,
                            'Pronunciation': pronun,
                            'Sound': '',
                            'English Meaning': '',
                            'POS': pos.lower(),
                            'Tags': '',
                        })
                else:
                    # apply contraction rule to noun_decl
                    noun_decl = apply_contraction(noun_decl)

                    # append
                    export_rows.append({
                        'Lemme': lemme,
                        'Noun Declension': noun_decl,
                        'Pronunciation': pronun,
                        'Sound': '',
                        'English Meaning': '',
                        'POS': pos.lower(),
                        'Tags': '',
                    })

        # output file name
        start_idx = freq_start + chunk_idx
        end_idx = start_idx + len(lemme_chunk) - 1
        out_file = os.path.join(
            OUTPUT_DIR, f'{OUTPUT_PREFIX}{start_idx}-{end_idx}.csv'
        )

        # Create DataFrame for export
        export_df = pd.DataFrame(export_rows)

        # Export CSV with UTF-8 and without index
        export_df.to_csv(out_file, index=False, encoding='utf-8')

        print(f'Formatting exceptions: {formatting_exception_count}\n')
        print(f'Exported {len(lemme_chunk)} lemme to {out_file}')


def group_rows_by_lemme(df) -> (list, dict):
    """
    Create a list of unique lexique lemmes.
    Create a dict of rows belonging to each lemme.
    Example:
        [ "lemme1", "lemme2", "lemme3" ]
        {"lemme1":[row_A, row_B], "lemme2":[row_C,...]}
    """
    unique_lemmes = []
    lemme_row_lookup = {}

    for idx, row in df.iterrows():
        lemme = row['lemme']
        if lemme not in lemme_row_lookup:
            lemme_row_lookup[lemme] = []
            unique_lemmes.append(lemme)
        lemme_row_lookup[lemme].append(row)

    return unique_lemmes, lemme_row_lookup

# Extract frequency start index from filename like 'Freq 1-500.csv
def parse_start_frequency(filename):
    match = re.search(r'Freq (\d+) - \d+', filename)
    if not match:
        raise ValueError('Invalid filename: ' + filename + '\n\nFilename is required to match format to accuracately determine frequency index.')
    return int(match.group(1))

# apply correct formatting rule based on pos and lemme.
def format_noun_declension(lemme, lemme_df, pos):
    # we'll just pre-compute this junk. it's a little inefficient but it reduces code complexity
    c_lemme_male = apply_contraction(f"le {lemme}")
    c_lemme_fem = apply_contraction(f"la {lemme}")

    # check for hard-coded exceptions first
    hard_coded_format = handle_hard_coded_formats(lemme_df, lemme, c_lemme_male, c_lemme_fem)
    if hard_coded_format is not None or hard_coded_format is False:
        return hard_coded_format

    if pos in {'ver', 'adv', 'pre', 'con', 'ono'}:
        return format_bold(lemme)
    elif pos == 'nom':
        return format_noun_declension_nom(lemme_df, lemme, c_lemme_male, c_lemme_fem)
    elif 'adj' in pos:
        return format_noun_declension_adj(lemme_df, lemme, c_lemme_male, c_lemme_fem)

    # If no rule matched
    return None


# pretty self explanatory
# note: returning false prevents duplicates from getting exported. DO NOT CHANGE THIS TO None.
def handle_hard_coded_formats(rows, lemme, c_lemme_male, c_lemme_fem):
    if lemme in {'quelque', 'quelques'}:
        if lemme == 'quelque':
            return f"<b>quelque</b> [<gr><i>pl. </i></gr>quelques]"
        else:
            return False

    # exceptions for archaic/rare poetic spellings
    elif lemme == 'oeil':
        return f"<b>l'oeil</b> [<gr><i>pl. </i></gr><blue>les yeux</blue>]"
    elif lemme == 'lieu':
        return f"<b>lieu</b> [<gr><i>pl. </i></gr>lieux]"

    elif lemme in HARD_CODED_ADJ_4_ROWS:
        if lemme == 'tout':
            return (f"<b>tout</b> ["
                    f"<gr><i>ms. </i></gr><blue>tout</blue>; "
                    f"<gr><i>mpl. </i></gr><blue>tous</blue>; "
                    f"<gr><i>fs. </i></gr><red>toute</red>; "
                    f"<gr><i>fpl. </i></gr><red>toutes</red>]")
        if lemme == 'aucun':
            return (f"<b>aucun</b> ["
                    f"<gr><i>ms. </i></gr><blue>aucun</blue>; "
                    f"<gr><i>mpl. </i></gr><blue>aucuns</blue>; "
                    f"<gr><i>fs. </i></gr><red>aucune</red>; "
                    f"<gr><i>fpl. </i></gr><red>aucunes</red>]")
        else:
            return False
    if lemme == SPECIAL_LEMME_FOIS:
        return '<b>la fois</b> [<gr><i>pl. </i></gr><red><b>les fois</b></red>]'

    return None

def format_bold(lemme):
    return f"<b>{lemme}</b>"


# handle 'nom' POS formatting with genre and nombre rules
# ...and some 'adj' stuff that should probably be abstracted
def format_noun_declension_nom(rows, lemme, c_lemme_male, c_lemme_fem):
    # helper to get row by genre and nombre with NaN checks
    def get_row(genre_val=None, nombre_val=None):
        cond = True
        if genre_val is None:
            cond &= rows['genre'].isna()
        else:
            cond &= rows['genre'] == genre_val
        if nombre_val is None:
            cond &= rows['nombre'].isna()
        else:
            cond &= rows['nombre'] == nombre_val
        subset = rows[cond]
        return subset.iloc[0] if not subset.empty else None

    try:
        # special case for 'fois'
        if lemme == SPECIAL_LEMME_FOIS:
            return f"<b><red>la {SPECIAL_LEMME_FOIS}<red></b> [<gr><i>pl. </i></gr><red>les {SPECIAL_LEMME_FOIS}</red>]"

        # single row cases
        if len(rows) == 1:
            row = rows.iloc[0]
            genre = row.get('genre', np.nan)
            nombre = row.get('nombre', np.nan)
            ortho = row['ortho']

            # assume this means only a plural form exists (e.g. you can have 'pants' but not 'pant')
            if nombre == "p":
                return f"<b>les {ortho}</b>"
            # else assume there is always both a single & plural form
            #  (this could be wrong if a word is singular only, that's prob infrequent)
            else:
                if genre == "m":
                    return f"<b><blue>{c_lemme_male}</blue></b> [<gr><i>pl. </i></gr><blue>les {ortho}</blue>]"
                elif genre == "f":
                    return f"<b><red>{c_lemme_fem}</red></b> [<gr><i>pl. </i></gr><red>les {ortho}</red>]"

        # two row cases
        elif len(rows) == 2:
            # infer missing 'nombre'
            if rows['nombre'].isna().sum() == 1:
                idx = rows[rows['nombre'].isna()].index[0]
                nombres = rows['nombre'].dropna()
                # if one is singular, then the other must be plural
                if (nombres == "s").sum() == 1:
                    rows.at[idx, 'nombre'] = "p"
                # if one is plural, then the other must be singular
                elif (nombres == "p").sum() == 1:
                    rows.at[idx, 'nombre'] = "s"
                else:
                    return None

            row1 = rows.iloc[0]
            row2 = rows.iloc[1]

            r1_genre = row1['genre']
            r1_nombre = row1['nombre']
            r2_genre = row2['genre']
            r2_nombre = row2['nombre']

            """
            Ahh french, there's just this: Je ne sais pas
             Wisely pronounced as Sheypa. (thanks French)
             
             This logic handles where for both rows genre is NaN but one row has 's' and one row has "p".
             The Lexique, in its grand wisdom, has decided this symobolizes words that are homographs and homophones but have different lemmas.
            
             They are semantically distinct words that just happen to share the same spelling and pronunciation but have different meanings depending on gender. 
             These will require their own flashcards and thus need multiple entries.
             Now we get to return lists - yay.
             
             For example:
             la tour (tower) v. le tour (turn)
             la livre (pound) v. le livre (book)
             la manche (sleeve/English Channel) v. le manche (handle)
             etc...
             
             There's a surprising amount of these, roughly 7 / 500. Thanks French. 
            """
            if pd.isna(row1['genre']) and pd.isna(row2['genre']) and r1_nombre == "s" and r2_nombre == "p" and row1["cgram"].lower() != 'adj':
                return [f"<b><blue>{c_lemme_male}</blue></b> [<gr><i>pl. </i></gr><blue>les {row2['ortho']}</blue>]",
                        f"<b><red>{c_lemme_fem}</red></b> [<gr><i>pl. </i></gr><red>les {row2['ortho']}</red>]"]

            if pd.isna(row1['genre']) and pd.isna(row2['genre']) and r1_nombre == "s" and r2_nombre == "p" and row1["cgram"].lower() == 'adj':
                return f"<b>{lemme}</b> [<gr><i>pl. </i></gr>{row2['ortho']}]"

            # discard nouns with conflicting genres
            if (r1_genre == "m" and r2_genre == "f") or (r1_genre == "f" and r2_genre == "m"):
                return None # no rule match
            # treat both rows as male, with first row 's' and second row "p"
            elif r1_genre == "m" or r2_genre == "m":
                if row1["cgram"].lower() == 'adj':
                    return f"<b>{lemme}</b> [<gr><i>mpl. </i></gr><blue>{row2['ortho']}</blue>]"
                else:
                    return f"<b><blue>{c_lemme_male}</blue></b> [<gr><i>pl. </i></gr><blue>les {row2['ortho']}</blue>]"
            # treat both rows as female, with first row 's' and second row "p"
            elif r1_genre == "f" or r2_genre == "f":
                if row1["cgram"].lower() == 'adj':
                    return f"<b>{lemme}</b> [<gr><i>fpl. </i></gr><red>{row2['ortho']}</red>]"
                else:
                    return f"<b><red>{c_lemme_fem}</red></b> [<gr><i>pl. </i></gr><red>les {row2['ortho']}</red>]"

        # my voluntary & entirely avoidable suffering is your flashcards
        elif len(rows) == 3:
            return noun_three(rows, lemme, c_lemme_male, c_lemme_fem)

        elif len(rows) == 4:
            return noun_four(rows, lemme)

        else:
            # if there's 5+ rows, then something is wrong, assume malformed and move on
            return None
    except ValueError or TypeError: # why is it so hard to figure out NaN errors :(
        return None


# handle 'adj' POS formatting with genre and nombre rules.
# rows: DataFrame subset for the lemme and POS = 'adj'
def format_noun_declension_adj(rows, lemme, c_lemme_male, c_lemme_fem):
    genre_vals = rows['genre'].dropna().unique()
    nombre_vals = rows['nombre'].dropna().unique()

    # if one row OR genre empty and all ortho's are equal then treat as single ver/adv style
    if len(rows) == 1 or (all(x == lemme for x in rows['ortho']) and rows['genre'].isna().all()):
        return format_bold(lemme)

    elif len(rows) == 4:
        # Expect ms, mpl, fs, fpl
        ms = find_row(rows, "m", "s")
        mpl = find_row(rows, "m", "p")
        fs = find_row(rows, "f", "s")
        fpl = find_row(rows, "f", "p")
        # nothing missing
        if ms is not None and mpl is not None and fs is not None and fpl is not None:
            return (
                f"<b>{lemme}</b> "
                f"[<gr><i>ms. </i></gr><blue>{ms['lemme']}</blue>; "
                f"<gr><i>mpl. </i></gr><blue>{mpl['lemme']}</blue>; "
                f"<gr><i>fs. </i></gr><red>{fs['lemme']}</red>; "
                f"<gr><i>fpl. </i></gr><red>{fpl['lemme']}</red>]"
            )
        # one or more rows missing
        else:
            # one row missing
            if ((ms is not None and mpl is not None and fs is not None) or (ms is not None and mpl is not None and fpl is not None) or (ms is not None and fs is not None and fpl is not None) or (mpl is not None and fs is not None and fpl is not None)):
                # assign ms|mpl|fs|fpl to row with missing genre/nombre malformed - process of elimination
                malformed_row = rows[rows['genre'].isna() | rows['nombre'].isna()]
                if ms is not None and mpl is not None and fs is not None:
                    fpl = malformed_row
                elif ms is not None and mpl is not None and fpl is not None:
                    fs = malformed_row
                elif ms is not None and fs is not None and fpl is not None:
                    mpl = malformed_row
                else:
                    ms = malformed_row

                # return corrected value
                return (
                    f"<b>{lemme}</b> "
                    f"[<gr><i>ms. </i></gr><blue>{ms['lemme']}</blue>; "
                    f"<gr><i>mpl. </i></gr><blue>{mpl['lemme']}</blue>; "
                    f"<gr><i>fs. </i></gr><red>{fs['lemme']}</red>; "
                    f"<gr><i>fpl. </i></gr><red>{fpl['lemme']}</red>]"
                )
            # todo could fix infer more fixes, for example if ms and fpl were both missing but there were rows with m_ and _pl

    # if two or three rows, apply same rules as 'nom'
    elif len(rows) == 2 or len(rows) == 3:
        result = format_noun_declension_nom(rows, lemme, c_lemme_male, c_lemme_fem)
        if result is not None:
            return result

    return None


def noun_three(df, lemme, c_lemme_male, c_lemme_fem):
    rows = df[['ortho', 'genre', 'nombre', "cgram"]].copy()

    # fail early if too many missing values
    if rows['nombre'].isna().sum() > 1 or rows['genre'].isna().sum() > 1:
        return None

    # infer missing 'nombre'
    if rows['nombre'].isna().any():
        idx = rows[rows['nombre'].isna()].index[0]
        nombres = rows['nombre'].dropna()
        # if we have 2 singulars, then the other must be plural
        if (nombres == "s").sum() == 2:
            rows.at[idx, 'nombre'] = "p"
        # if we have 1 singular and 1 plural, then the other must be singular
        elif (nombres == "p").sum() == 1 and (nombres == "s").sum() == 1:
            rows.at[idx, 'nombre'] = "s"
        else:
            return None

    # infer missing 'genre' (only if 'nombre' is 's')
    if rows['genre'].isna().any():
        idx = rows[rows['genre'].isna()].index[0]
        if rows.at[idx, 'nombre'] == "p":
            pass  # genre doesn't matter for plural
        else:
            other = rows[(rows.index != idx) & (rows['nombre'] == "s")]
            genres = other['genre'].dropna().unique()
            if len(genres) == 1:
                rows.at[idx, 'genre'] = "f" if genres[0] == "m" else "m"
            else:
                return None

    # identify forms
    masc_sing = rows[(rows['genre'] == "m") & (rows['nombre'] == 's')]
    fem_sing = rows[(rows['genre'] == "f") & (rows['nombre'] == 's')]
    plural = rows[rows['nombre'] == "p"]

    if len(masc_sing) != 1 or len(plural) != 1:
        return None
    if len(fem_sing) > 1:
        return None

    ortho_m = masc_sing.iloc[0]['ortho']
    ortho_p = plural.iloc[0]['ortho']
    ortho_f = fem_sing.iloc[0]['ortho'] if not fem_sing.empty else None

    # if both plural and feminine are the same as masculine, return None
    if (ortho_p == ortho_m) and (ortho_f is None or ortho_f == ortho_m):
        return None

    # only format if both forms differ
    pos = rows.iloc[0]["cgram"].lower()
    if ortho_p != ortho_m and ortho_f and ortho_f != ortho_m:
        if pos == 'adj':
            return f"<b>{lemme}</b> [<gr><i>m. </i></gr><blue>{ortho_m}</blue>; <gr><i>pl. </i></gr><blue>{ortho_p}</blue>; <gr><i>f. </i></gr><red>{ortho_f}</red>]"
        else:
            return f"<b><blue>{c_lemme_male}</blue></b> [<gr><i>pl. </i></gr><blue>les {ortho_p}</blue>; <gr><i>f. </i></gr><red>la {ortho_f}</red>]"
    elif ortho_p != ortho_m and (ortho_f is None or ortho_f == ortho_m):
        if pos == 'adj':
            return f"<b>{lemme}</b> [<gr><i>s. </i></gr><blue>{ortho_m}</blue>; <gr><i>pl. </i></gr><blue>{ortho_p}</blue>]"
        else:
            return f"<b><blue>{c_lemme_male}</blue></b> [<gr><i>pl. </i></gr><blue>les {ortho_p}</blue>]"
    elif ortho_f and ortho_f != ortho_m and ortho_p == ortho_m:
        if pos == 'adj':
            return f"<b>{lemme}</b> [<gr><i>m. </i></gr><blue>{ortho_m}</blue>; <gr><i>pl. </i></gr><blue>{ortho_p}</blue>; <gr><i>f. </i></gr><red>{ortho_f}</red>]"
        else:
            return f"<b><red>{c_lemme_fem}</red></b> [<gr><i>pl. </i></gr><red>les {ortho_p}</red>]"
    else:
        return None


def noun_four(df, lemme):
    # work on a copy of relevant columns
    rows = df[['ortho', 'genre', 'nombre']].copy()

    # count missing fields per row
    missing_genre_mask = rows['genre'].isna()
    missing_nombre_mask = rows['nombre'].isna()
    missing_both_mask = missing_genre_mask & missing_nombre_mask

    # total missing counts
    total_missing_genre = missing_genre_mask.sum()
    total_missing_nombre = missing_nombre_mask.sum()
    total_missing_both = missing_both_mask.sum()

    # sanity checks
    if total_missing_both > 1:
        return None  # More than one row missing both - ambiguous

    # if there is one row missing both genre and nombre
    if total_missing_both == 1:
        # check that the other three rows have no missing fields
        others = rows[~missing_both_mask]
        if others['genre'].isna().any() or others['nombre'].isna().any():
            return None  # Others must be complete

        # check others cover 3 distinct (genre,nombre) combos
        combos = set(zip(others['genre'], others['nombre']))
        if len(combos) != 3:
            return None  # Not unique combos, can't infer

        # determine the missing combo (genre,nombre)
        expected_combos = {('m', 's'), ('m', 'p'), ('f', 's'), ('f', 'p')}
        missing_combo = expected_combos - combos
        if len(missing_combo) != 1:
            return None  # Ambiguous missing combo

        missing_genre, missing_nombre = missing_combo.pop()
        # assign missing fields to the missing_both row
        idx = rows[missing_both_mask].index[0]
        rows.at[idx, 'genre'] = missing_genre
        rows.at[idx, 'nombre'] = missing_nombre

    else:
        # no rows missing both fields
        # handle missing single fields (genre or nombre)

        # infer missing nombre if exactly one missing
        if total_missing_nombre == 1:
            idx = rows[missing_nombre_mask].index[0]
            existing_nombres = set(rows.loc[~missing_nombre_mask, 'nombre'])
            expected_nombres = {'s', 'p'}
            missing_nombre_values = expected_nombres - existing_nombres
            if len(missing_nombre_values) != 1:
                return None
            rows.at[idx, 'nombre'] = missing_nombre_values.pop()

        # infer missing genre if exactly one missing
        if total_missing_genre == 1:
            idx = rows[missing_genre_mask].index[0]
            row_nombre = rows.at[idx, 'nombre']
            if row_nombre not in {'s', "p"}:
                return None  # nombre must be known to infer genre
            same_nombre_rows = rows[(rows.index != idx) & (rows['nombre'] == row_nombre)]
            existing_genres = set(same_nombre_rows['genre'].dropna())
            expected_genres = {'m', 'f'}
            missing_genres = expected_genres - existing_genres
            if len(missing_genres) != 1:
                return None
            rows.at[idx, 'genre'] = missing_genres.pop()

    # after inference, if any genre or nombre is still missing, return None
    if rows['genre'].isna().any() or rows['nombre'].isna().any():
        return None

    # validate that all (genre, nombre) combinations are unique and complete
    combos_seen = set()
    groups = {
        ('m', 's'): None,
        ('m', "p"): None,
        ('f', 's'): None,
        ('f', "p"): None,
    }

    for _, row in rows.iterrows():
        key = (row['genre'], row['nombre'])
        if key not in groups:
            return None  # invalid genre/number combo
        if groups[key] is not None:
            return None  # duplicate combo
        groups[key] = row['ortho']

    # if any combo missing, return None
    if any(v is None for v in groups.values()):
        return None

    # format final string
    return (
        f"<b>{lemme}</b> ["
        f"<gr><i>ms. </i></gr> <blue>le {groups[("m", 's')]}</blue>; "
        f"<gr><i>mpl. </i></gr> <blue>les {groups[("m", "p")]}</blue>; "
        f"<gr><i>fs. </i></gr> <red>la {groups[("f", 's')]}</red>; "
        f"<gr><i>fpl. </i></gr> <red>les {groups[("f", "p")]}</red>"
        "]"
    )


# returns the first row genre and nombre equal the inputs
# None may be passed as NaN
def find_row(rows, g, n):
    if g is None and n is None:
        r = rows[(rows['genre'].isna()) & (rows['nombre'].isna())]
        return r.iloc[0] if not r.empty else None
    elif g is None:
        r = rows[(rows['genre'].isna()) & (rows['nombre'] == n)]
        return r.iloc[0] if not r.empty else None
    elif n is None:
        r = rows[(rows['genre'] == g) & (rows['nombre'].isna())]
        return r.iloc[0] if not r.empty else None
    else:
        r = rows[(rows['genre'] == g) & (rows['nombre'] == n)]
        return r.iloc[0] if not r.empty else None


# returns formatted article + word (ortho or lemme) w/ (f) if applicable
# for male: return le + word | l'word
# for fem:  return la + word | l'word (f)
def apply_contraction(text) -> str:
    def repl(m):
        article = m.group(1)
        word = m.group(2)
        vowels = "aeiouhâàéèêëïîôùûü"
        if word and word[0].lower() in vowels and article == "le":
            return f"l'{word}"
        elif word and word[0].lower() in vowels and article == "la":
            return f"l'{word} (f)"
        else:
            return m.group(0)

    pattern = re.compile(r"<b>(le|la|les) (\S+?)</b>")
    return pattern.sub(repl, text)


if __name__ == "__main__":
    main()


"""
This comment contains the if-statement that makes sense. I used to some substitution plus
DeMorgan's Law to convert that into the monstrous if-statement you see below this comment.

This was necessary because we cannot check if a genre is "f" or "m" without first checking
within its own local evaluated conditional that r1_genre is not NaN.

This 'simplification' allows us to handle empty excel cells without errors.

# if not ((r1_genre == "m" and r2_genre == "f") or (r1_genre == "f" and r2_genre == "m")):

if (((pd.isna(r1_genre) or r1_genre == "f") or (pd.isna(r2_genre) or r2_genre == "f")) and
        ((pd.isna(r1_genre) or r1_genre == "m") or (pd.isna(r2_genre) or r2_genre == "f"))):
        
AHAHAHA HAH! We can just try: except: for NaN errors. Rip.
"""
