# detector.py
import numpy as np
import pandas as pd
from config import TAG_PAIRS, ANOMALY_STD_MULTIPLIER


from logger_config import setup_logger
logger = setup_logger(__name__)

def detect_anomalies_for_pair(df: pd.DataFrame, sp_tag: str, pv_tag: str) -> pd.DataFrame:
    sp_col = f"SetPoint_{sp_tag}"
    pv_col = f"Actual_{pv_tag}"
    err_col = f"Error_{sp_tag}"
    anomaly_col = f"Anomaly_{sp_tag}"

    if sp_col not in df.columns or pv_col not in df.columns:
        logger.warning(f"Missing required columns: {sp_col}, {pv_col}")
        return df

    df[err_col] = df[sp_col] - df[pv_col]

    mean = df[err_col].mean()
    std = df[err_col].std()
    threshold = ANOMALY_STD_MULTIPLIER * std

    df[anomaly_col] = np.abs(df[err_col] - mean) > threshold
    logger.info(f"Anomalies detected for {sp_tag}: {df[anomaly_col].sum()} rows")

    return df, df[anomaly_col].any()