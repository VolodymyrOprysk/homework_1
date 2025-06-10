import os
import pandas as pd
from sqlalchemy import create_engine, text

from src.helpers import copy_csv_to_docker_container, create_db_tables, delete_csv_from_docker_container
from src.logger import get_logger

chunk_size = 1_000_000
logger = get_logger()

# --- Precreate tables ---
engine = create_engine(
    "mysql+pymysql://etl_user:etlpass@mysql-etl:3306/etl_db?local_infile=1"
)

logger.info("Created DB engine")

create_db_tables(engine)

logger.info("Created tables.")
# --- Extract ---
logger.info("Extracting CSV...")
users_csv_df = pd.read_csv('data/users.csv')
campaigns_csv_df = pd.read_csv('data/campaigns.csv')
logger.info("CSV extracted")

# --- USERS ---

logger.info("Processing users.csv...")

# Genders
genders_df = pd.DataFrame(users_csv_df['Gender'].unique(), columns=['Gender'])
genders_df['GenderID'] = range(1, len(genders_df) + 1)
genders_map = dict(zip(genders_df['Gender'], genders_df['GenderID']))
logger.info(f"Prepared genders_df ({len(genders_df)})")
genders_df.to_sql('Genders', engine, if_exists='append',
                  index=False, method='multi')
logger.info("Inserted genders_df into DB")

# Locations
locations_df = pd.DataFrame(
    users_csv_df['Location'].unique(), columns=['Location'])
locations_df['LocationID'] = range(1, len(locations_df) + 1)
locations_map = dict(zip(locations_df['Location'], locations_df['LocationID']))
logger.info(f"Prepared locations_df ({len(locations_df)})")
locations_df.to_sql('Locations', engine, if_exists='append',
                    index=False, method='multi')
logger.info("Inserted locations_df into DB")

# Interests
interests_df = (
    users_csv_df['Interests']
    .str.split(',')
    .explode()
    .str.strip()
    .drop_duplicates()
    .to_frame(name='Interest')
)
interests_df['InterestID'] = range(1, len(interests_df) + 1)
interests_map = dict(zip(interests_df['Interest'], interests_df['InterestID']))
logger.info(f"Prepared interests_df ({len(interests_df)})")
interests_df.to_sql('Interests', engine, if_exists='append',
                    index=False, method='multi')
logger.info("Inserted interests_df into DB")

# Users
users_df = users_csv_df[['UserID', 'Age', 'Gender', 'Location', 'SignupDate']]
users_df = users_df.assign(GenderID=users_df['Gender'].replace(
    genders_map)).drop(columns=['Gender'])
users_df = users_df.assign(LocationID=users_df['Location'].replace(
    locations_map)).drop(columns=['Location'])
logger.info(f"Prepared users_df ({len(users_df)})")
users_df.to_sql('Users', engine, if_exists='append',
                index=False, chunksize=chunk_size, method='multi')
logger.info("Inserted users_df into DB")

# UsersInterests
user_interests = []
for _, row in users_csv_df[['UserID', 'Interests']].iterrows():
    interests = [i.strip() for i in row['Interests'].split(',')]
    for interest in interests:
        if interest in interests_map:
            user_interests.append((row['UserID'], interests_map[interest]))
users_interests_df = pd.DataFrame(
    user_interests, columns=['UserID', 'InterestID'])
users_interests_df['UserInterestID'] = range(1, len(users_interests_df) + 1)
logger.info(f"Prepared users_interests_df ({len(users_interests_df)})")
users_interests_df.to_sql('UsersInterests', engine,
                          if_exists='append', index=False, chunksize=chunk_size, method='multi')
logger.info("Inserted users_interests_df into DB")

logger.info("users.csv processing finished")

# --- CAMPAIGNS ---

logger.info("Processing campaigns.csv...")

advertisers_df = pd.DataFrame(
    campaigns_csv_df['AdvertiserName'].unique(), columns=['Advertiser'])
advertisers_df['AdvertiserID'] = range(1, len(advertisers_df) + 1)
advertisers_map = dict(
    zip(advertisers_df['Advertiser'], advertisers_df['AdvertiserID']))
logger.info(f"Prepared advertisers_df ({len(advertisers_df)})")
advertisers_df.to_sql('Advertisers', engine,
                      if_exists='append', index=False, method='multi')
logger.info("Inserted advertisers_df into DB")

ad_slots_df = pd.DataFrame(
    campaigns_csv_df['AdSlotSize'].unique(), columns=['AdSlotSize'])
ad_slots_df['AdSlotSizeID'] = range(1, len(ad_slots_df) + 1)
ad_slots_df['AdSlotWidth'] = ad_slots_df['AdSlotSize'].str.split('x').str[0]
ad_slots_df['AdSlotHeight'] = ad_slots_df['AdSlotSize'].str.split('x').str[1]
ad_slots_map = dict(
    zip(ad_slots_df['AdSlotSize'], ad_slots_df['AdSlotSizeID']))
ad_slots_df = ad_slots_df.drop(columns='AdSlotSize')
logger.info(f"Prepared ad_slots_df ({len(ad_slots_df)})")
ad_slots_df.to_sql('AdSlotSizes', engine, if_exists='append',
                   index=False, method='multi')
logger.info("Inserted ad_slots_df into DB")

campaigns_df = campaigns_csv_df[['CampaignID', 'CampaignName', 'AdvertiserName', 'CampaignStartDate',
                                 'CampaignEndDate', 'AdSlotSize', 'TargetingCriteria', 'Budget', 'RemainingBudget']]
campaigns_df = campaigns_df.assign(AdvertiserID=campaigns_df['AdvertiserName'].replace(
    advertisers_map)).drop(columns=['AdvertiserName'])
campaigns_df = campaigns_df.assign(AdSlotSizeID=campaigns_df['AdSlotSize'].replace(
    ad_slots_map)).drop(columns=['AdSlotSize'])
campaigns_df['TargetAgeStart'] = campaigns_df['TargetingCriteria'].str.split(
    ',').str[0].str.replace('Age', '').str.split('-').str[0].str.strip()
campaigns_df['TargetAgeEnd'] = campaigns_df['TargetingCriteria'].str.split(
    ',').str[0].str.split('-').str[1].str.strip()
campaigns_df = campaigns_df.drop(columns='TargetingCriteria')
campaigns_map = dict(
    zip(campaigns_df['CampaignName'], campaigns_df['CampaignID']))
logger.info(f"Prepared campaigns_df ({len(campaigns_df)})")
campaigns_df.to_sql('Campaigns', engine, if_exists='append',
                    index=False, method='multi')
logger.info("Inserted campaigns_df into DB")

campaigns_targets_df = pd.DataFrame(
    campaigns_csv_df[['CampaignID', 'TargetingCriteria']])
campaigns_targets_df['InterestID'] = campaigns_targets_df['TargetingCriteria'].str.split(
    ',').str[1].str.strip().replace(interests_map)
campaigns_targets_df['LocationID'] = campaigns_targets_df['TargetingCriteria'].str.split(
    ',').str[2].str.strip().replace(locations_map)

campaigns_targeting_interests_df = pd.DataFrame(
    campaigns_targets_df[['CampaignID', 'InterestID']])
campaigns_targeting_interests_df['CampaignTargetInterestID'] = range(
    1, len(campaigns_targeting_interests_df) + 1)
logger.info(
    f"Prepared campaigns_targeting_interests_df ({len(campaigns_targeting_interests_df)})")
campaigns_targeting_interests_df.to_sql(
    'CampaignsTargetingInterests', engine, if_exists='append', index=False, method='multi')
logger.info("Inserted campaigns_targeting_interests_df into DB")

campaigns_targeting_locations_df = campaigns_targets_df[[
    'CampaignID', 'LocationID']]
campaigns_targeting_locations_df['CampaignTargetLocationID'] = range(
    1, len(campaigns_targeting_locations_df) + 1)
logger.info(
    f"Prepared campaigns_targeting_locations_df ({len(campaigns_targeting_locations_df)})")
campaigns_targeting_locations_df.to_sql(
    'CampaignsTargetingLocations', engine, if_exists='append', index=False, method='multi')
logger.info("Inserted campaigns_targeting_locations_df into DB")

logger.info("campaigns.csv processing finished")

# --- AD EVENTS (chunked) ---

unique_devices = set()
processed_chunks = []
records_count = 0

# Read column names for ad_events.csv
with open('data/ad_events.csv') as f:
    columns_line = f.readline()
    columns_names = columns_line.replace('TargetingCriteria',
                                         'TargetingAge,TargetingInterests,TargetingCriteria').split(',')

dtype_map = {
    9: 'int',    # UserID
    13: 'float',  # BidAmount
    14: 'float',  # AdCost
    15: 'bool',   # WasClicked
    17: 'float',  # AdRevenue
}

# First pass: collect unique devices, store chunks
for chunk in pd.read_csv('data/ad_events.csv', chunksize=chunk_size, header=None, names=columns_names, skiprows=1, dtype=dtype_map):
    records_count += 1
    logger.info(f"Processing chunk #{records_count}...")
    unique_devices.update(chunk['Device'].unique())
    processed_chunks.append(chunk)

# Devices
devices_df = pd.DataFrame(sorted(unique_devices), columns=['Device'])
devices_df['DeviceID'] = range(1, len(devices_df) + 1)
devices_map = dict(zip(devices_df['Device'], devices_df['DeviceID']))
logger.info(f"Prepared devices_df ({len(devices_df)})")
devices_df.to_sql('Devices', engine, if_exists='append',
                  index=False, method='multi')
logger.info("Inserted devices_df into DB")


for i, chunk in enumerate(processed_chunks, start=1):
    logger.info(f"Transforming chunk #{i} out of {records_count}...")

    ad_events_df = chunk[['EventID', 'CampaignName', 'UserID', 'Device', 'Timestamp',
                          'BidAmount', 'AdCost', 'WasClicked', 'ClickTimestamp', 'AdRevenue']]
    ad_events_df = ad_events_df.assign(
        CampaignID=ad_events_df['CampaignName'].replace(campaigns_map)
    ).drop(columns=['CampaignName'])
    ad_events_df = ad_events_df.assign(
        DeviceID=ad_events_df['Device'].replace(devices_map)
    ).drop(columns=['Device'])

    ad_events_df['WasClicked'] = ad_events_df['WasClicked'].astype(int)
    ad_events_df['ClickTimestamp'] = pd.to_datetime(
        ad_events_df['ClickTimestamp'], errors='coerce')

    ad_events_df.rename(columns={'EventID': 'AdEventID'}, inplace=True)

    local_csv_path = f'data/ad_events_chunk_{i}.csv'

    logger.info(f"Writing chunk #{i} to CSV: {local_csv_path}")
    ad_events_df.to_csv(local_csv_path, index=False, na_rep='\\N')

    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        try:
            conn.execute(
                text(f"""
                    LOAD DATA LOCAL INFILE '{local_csv_path}'
                    INTO TABLE AdEvents
                    FIELDS TERMINATED BY ','
                    ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 LINES
                    (AdEventID, UserID, Timestamp, BidAmount, AdCost,
                     WasClicked, ClickTimestamp, AdRevenue, CampaignID, DeviceID)
                """)
            )

        finally:
            os.remove(local_csv_path)

        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

    logger.info(f"Loaded chunk #{i} into AdEvents table")
