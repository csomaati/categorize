import pandas as pd

def action_update(row: pd.Series, action_params, properties):
    for row_name, row_content in action_params.items():
        if row_name not in row: continue
        row[row_name] = row_content.format(**properties)
    new_df = pd.DataFrame([row,])
    return new_df
