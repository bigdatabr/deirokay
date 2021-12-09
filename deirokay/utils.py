from difflib import get_close_matches


def _check_columns_in_df_columns(columns, df_columns):
    miss = {}
    for col in columns:
        if col not in df_columns:
            match = get_close_matches(col, df_columns, n=1, cutoff=0)[0]
            miss[col] = match
    if miss:
        raise KeyError(f'Columns {list(miss.keys())} not found in your data.'
                       f' Did you mean {list(miss.values())}?')
