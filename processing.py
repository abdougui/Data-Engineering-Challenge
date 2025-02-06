import logging
import pandas as pd
from datetime import timedelta

def resample_and_impute(df: pd.DataFrame, time_col: str, value_col: str, freq: str = '15T', max_gap_hours: int = 2) -> pd.DataFrame:
    df[time_col] = pd.to_datetime(df[time_col], dayfirst=True, errors='coerce')
    df = df.dropna(subset=[time_col])
    df.set_index(time_col, inplace=True)

    if not df.index.is_unique:
        df = df.groupby(df.index).mean()

    df_res = df.resample(freq).asfreq()
    df_res['interpolated'] = df_res[value_col].interpolate(method='time')

    is_nan = df_res[value_col].isnull()
    gap_groups = (is_nan != is_nan.shift()).cumsum()

    for _, group_df in df_res.groupby(gap_groups):
        if group_df[value_col].isnull().all():
            gap_duration = group_df.index[-1] - group_df.index[0]
            if gap_duration <= timedelta(hours=max_gap_hours):
                df_res.loc[group_df.index, value_col] = df_res.loc[group_df.index, 'interpolated']
    
    df_res.drop(columns=['interpolated'], inplace=True)
    return df_res

def process_energy_data(energy_data: dict) -> dict:
    processed = {}
    for sheet, df in energy_data.items():
        if df.shape[1] < 2:
            logging.warning("Sheet '%s' has <2 columns; skipping.", sheet)
            continue
        
        time_col = df.columns[0]
        hvac_col = df.columns[1]
        df.rename(columns={hvac_col: "HVAC_Power_kW"}, inplace=True)
        df = df[[time_col, "HVAC_Power_kW"]].copy()

        processed_df = resample_and_impute(df, time_col, "HVAC_Power_kW", freq='15T', max_gap_hours=2)
        processed[sheet] = processed_df
    
    return processed

def generate_data_quality_report(processed_data: dict) -> str:
    lines = []
    for sheet, df in processed_data.items():
        lines.append(f"Sheet: {sheet}")
        if df.empty:
            lines.append("  (Empty DataFrame).")
            continue
        total_points = len(df)
        missing = df['HVAC_Power_kW'].isna().sum()
        quality_score = (1 - missing / total_points) * 100 if total_points else 0
        lines.append(f"  Total Points: {total_points}")
        lines.append(f"  Missing: {missing}")
        lines.append(f"  Quality Score: {quality_score:.2f}%")
        lines.append(f"  Start: {df.index.min()} | End: {df.index.max()}")
    return "\n".join(lines)
