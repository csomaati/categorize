import pandas  as pd
import functools
import random
import string

def rnd(length):
    # Random string with the combination of lower and upper case
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(length))
    return result_str

def extendable(func):
    @functools.wraps(func)
    def wrap_extend(df: pd.DataFrame, *args, **kwargs):
      row_name = rnd(8)
      df[row_name] = range(1, len(df.index)+1)
      try:
          groups = df.groupby(row_name, group_keys=False)
          df = groups.apply(func, *args, **kwargs)
      finally:
          df.drop(row_name, axis=1, inplace=True)
      return df

    return wrap_extend
