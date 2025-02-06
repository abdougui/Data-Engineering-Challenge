import logging
import yaml
import time
import pandas as pd

from ingestion import load_energy_data, load_carbon_footprint_data
from processing import process_energy_data, generate_data_quality_report
from db_loader import (
    create_primary_tables, 
    load_data_to_db,
)

def load_config(config_file='config.yaml'):
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    overall_start = time.time()

    # 1. Load config
    config = load_config()

    # 2. Ingest data
    ingestion_start = time.time()
    energy_data = load_energy_data(config['data']['energy_excel'])
    carbon_df = load_carbon_footprint_data(
        energy_data, 
        file_path=config['data'].get('carbon_data_url')
    )
    logging.info("Data ingestion took %.2f seconds.", time.time() - ingestion_start)

    # 3. Process energy data (15-min resampling)
    processing_start = time.time()
    processed_energy = process_energy_data(energy_data)
    dq_report = generate_data_quality_report(processed_energy)
    logging.info("Data quality report:\n%s", dq_report)
    logging.info("Data processing took %.2f seconds.", time.time() - processing_start)

    power_df = pd.DataFrame(columns=["timestamp", "hvac_power_kw"])
    if "HVAC" in processed_energy:
        hvac_df = processed_energy["HVAC"].reset_index()
        hvac_df.rename(columns={...}, inplace=True)
        power_df = hvac_df
    else:
        power_df = pd.DataFrame(columns=["timestamp", "hvac_power_kw"])

    # CFP DATA (hourly)
    cfp_df = carbon_df.copy()
    # ensure columns match DB schema, e.g. 'timestamp' is lowercase
    if 'timestamp' in cfp_df.columns:
        cfp_df['timestamp'] = pd.to_datetime(cfp_df['timestamp'], errors='coerce')
    else:
        # fallback if no 'timestamp' column
        cfp_df['timestamp'] = pd.NaT  

    # TEMPERATURE DATA
    temperature_df = pd.DataFrame(columns=["timestamp", "temperature"])

    engine = create_primary_tables(config['database'])

    load_data_to_db(engine, power_df, cfp_df, temperature_df)

    logging.info("Entire pipeline took %.2f seconds.", time.time() - overall_start)

if __name__ == "__main__":
    main()
