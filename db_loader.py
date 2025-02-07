import logging
import pandas as pd
from sqlalchemy import create_engine, Column, DateTime, Float, String, MetaData, Table

def create_primary_tables(db_config: dict):
    connection_string = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    engine = create_engine(connection_string)
    metadata = MetaData()

    Table('Power_Table', metadata,
          Column('timestamp', DateTime, primary_key=True),
          Column('HVAC_Power_kW', Float, nullable=False))

    Table('CFP_Table', metadata,
          Column('timestamp', DateTime, primary_key=True),
          Column('renewable_biomass', Float),
          Column('renewable_hydro', Float),
          Column('renewable_solar', Float),
          Column('renewable_wind', Float),
          Column('renewable_geothermal', Float),
          Column('renewable_otherrenewable', Float),
          Column('renewable', Float),
          Column('nonrenewable_coal', Float),
          Column('nonrenewable_gas', Float),
          Column('nonrenewable_nuclear', Float),
          Column('nonrenewable_oil', Float),
          Column('nonrenewable', Float),
          Column('hydropumpedstorage', Float),
          Column('unknown', Float),
          Column('region_id', String),
          Column('country_id', String))

    Table('Temperature_Table', metadata,
          Column('timestamp', DateTime, primary_key=True),
          Column('temperature', Float))

    metadata.create_all(engine)
    logging.info("Created primary tables if not existing.")
    return engine

def load_data_to_db(engine, power_df: pd.DataFrame, cfp_df: pd.DataFrame, temperature_df: pd.DataFrame):
    logging.info("Loading data into Power_Table...")
    power_df.to_sql('Power_Table', engine, if_exists='append', index=False)
    logging.info("Inserted %d rows into Power_Table.", len(power_df))

    logging.info("Loading data into CFP_Table...")
    cfp_df.to_sql('CFP_Table', engine, if_exists='append', index=False)
    logging.info("Inserted %d rows into CFP_Table.", len(cfp_df))

    logging.info("Loading data into Temperature_Table...")
    temperature_df.to_sql('Temperature_Table', engine, if_exists='append', index=False)
    logging.info("Inserted %d rows into Temperature_Table.", len(temperature_df))