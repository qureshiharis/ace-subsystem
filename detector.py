# detector.py
import numpy as np
import pandas as pd
from config import TAG_PAIRS, ANOMALY_STD_MULTIPLIER

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def detect_anomalies(df_combined):
    logger.info("Starting anomaly detection.")
    anomalies = {}

    for sp_tag, pv_tag in TAG_PAIRS:
        sp_col = f"SetPoint_{sp_tag}"
        pv_col = f"Actual_{pv_tag}"
        err_col = f"Error_{sp_tag}"

        if sp_col in df_combined.columns and pv_col in df_combined.columns:
            df_combined[err_col] = df_combined[sp_col] - df_combined[pv_col]

            logger.debug(f"Processing tag pair: {sp_tag}, {pv_tag}")
            logger.debug(f"{sp_col} - {pv_col} = {err_col}")

            mean = df_combined[err_col].mean()
            std = df_combined[err_col].std()
            threshold = ANOMALY_STD_MULTIPLIER * std

            logger.debug(f"Mean error: {mean}, Std: {std}, Threshold: {threshold}")

            anomaly_col = f"Anomaly_{sp_tag}"
            df_combined[anomaly_col] = np.abs(df_combined[err_col] - mean) > threshold

            logger.info(f"Anomalies detected for {sp_tag}: {df_combined[anomaly_col].sum()} rows")

            anomalies[sp_tag] = df_combined[anomaly_col].any() if not df_combined.empty else False
        else:
            logger.warning(f"Missing columns for tag pair: {sp_col}, {pv_col}")
            anomalies[sp_tag] = False

    logger.info("Anomaly detection complete.")
    return df_combined, anomalies
