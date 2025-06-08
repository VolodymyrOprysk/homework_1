import pandas as pd
from sqlalchemy import create_engine, text

from db_schema_setup import create_db_tables
from loggeeer import get_logger

chunk_size = 50_000
logger = get_logger()

# --- Precreate tables ---
engine = create_engine(
    "mysql+pymysql://etl_user:etlpass@localhost:3306/etl_db"
)

logger.info("Created DB engine")

create_db_tables(engine)


# --- Extract ---
logger.info("Extracting CSV...")
users_csv_df = pd.read_csv('data/users.csv')
campaigns_csv_df = pd.read_csv('data/campaigns.csv')
# ad_events_csv_df = pd.read_csv('data/ad_events.csv', chunksize=chunk_size)
logger.info("CSV extracted")
# --- Transform ---

# region Users CSV
logger.info("Processing users.csv...")
# Genders
genders_df = pd.DataFrame(users_csv_df['Gender'].unique(), columns=['Gender'])
genders_df['GenderID'] = range(1, len(genders_df) + 1)
genders_map = dict(zip(genders_df['Gender'], genders_df['GenderID']))

# Locations
locations_df = pd.DataFrame(
    users_csv_df['Location'].unique(), columns=['Location'])
locations_df['LocationID'] = range(1, len(locations_df) + 1)
locations_map = dict(zip(locations_df['Location'], locations_df['LocationID']))

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

# Users
users_df = users_csv_df[['UserID', 'Age', 'Gender', 'Location', 'SignupDate']]

users_df = users_df.assign(
    GenderID=users_df['Gender'].replace(genders_map)
).drop(columns=['Gender'])

users_df = users_df.assign(
    LocationID=users_df['Location'].replace(locations_map)
).drop(columns=['Location'])


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

logger.info("users.csv processing finished")

# endregion

logger.info("Processing campaigns.csv...")


advertisers_df = pd.DataFrame(
    campaigns_csv_df['AdvertiserName'].unique(), columns=['Advertiser'])
advertisers_df['AdvertiserID'] = range(1, len(advertisers_df) + 1)
advertisers_map = dict(
    zip(advertisers_df['Advertiser'], advertisers_df['AdvertiserID']))

ad_slots_df = pd.DataFrame(
    campaigns_csv_df['AdSlotSize'].unique(), columns=['AdSlotSize'])
ad_slots_df['AdSlotSizeID'] = range(1, len(ad_slots_df) + 1)
ad_slots_df['AdSlotWidth'] = campaigns_csv_df['AdSlotSize'].str.split(
    'x').str[0]
ad_slots_df['AdSlotHeight'] = campaigns_csv_df['AdSlotSize'].str.split(
    'x').str[1]
ad_slots_map = dict(
    zip(ad_slots_df['AdSlotSize'], ad_slots_df['AdSlotSizeID']))
ad_slots_df = ad_slots_df.drop(columns='AdSlotSize')

campaigns_df = pd.DataFrame(
    campaigns_csv_df[['CampaignID', 'CampaignName', 'AdvertiserName', 'CampaignStartDate',
                      'CampaignEndDate', 'AdSlotSize', 'TargetingCriteria', 'Budget', 'RemainingBudget']]
)

campaigns_df = campaigns_df.assign(
    AdvertiserID=campaigns_df['AdvertiserName'].replace(advertisers_map)
).drop(columns=['AdvertiserName'])

campaigns_df = campaigns_df.assign(
    AdSlotSizeID=campaigns_df['AdSlotSize'].replace(ad_slots_map)
).drop(columns=['AdSlotSize'])

campaigns_df['TargetAgeStart'] = campaigns_df['TargetingCriteria'].str.split(
    ',').str[0].str.replace('Age', '').str.split('-').str[0].str.strip()
campaigns_df['TargetAgeEnd'] = campaigns_df['TargetingCriteria'].str.split(
    ',').str[0].str.split('-').str[1].str.strip()

campaigns_df = campaigns_df.drop(columns='TargetingCriteria')
campaigns_map = dict(
    zip(campaigns_df['CampaignName'], campaigns_df['CampaignID'])
)

campaigns_targets_df = pd.DataFrame(
    campaigns_csv_df[['CampaignID', 'TargetingCriteria']])

campaigns_targets_df['InterestID'] = campaigns_targets_df['TargetingCriteria'].str.split(
    ',').str[1].str.strip().replace(interests_map)

campaigns_targets_df['LocationID'] = campaigns_targets_df['TargetingCriteria'].str.split(
    ',').str[2].str.strip().replace(locations_map)

campaigns_targeting_interests_df = pd.DataFrame(campaigns_targets_df[[
    'CampaignID', 'InterestID']])
campaigns_targeting_interests_df['CampaignTargetInterestID'] = range(
    1, len(campaigns_targeting_interests_df) + 1)

campaigns_targeting_locations_df = campaigns_targets_df[[
    'CampaignID', 'LocationID']]
campaigns_targeting_locations_df['CampaignTargetLocationID'] = range(
    1, len(campaigns_targeting_locations_df) + 1)

logger.info("campaigns.csv processing finished")

unique_devices = set()
processed_chunks = []
records_count = 0
transformed_count = 0
columns_names = []

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

for chunk in pd.read_csv('data/ad_events.csv', chunksize=chunk_size, header=None, names=columns_names, skiprows=1, dtype=dtype_map):
    records_count += 1
    logger.info(f"Processing chunk #{records_count}...")

    unique_devices.update(chunk['Device'].unique())

    processed_chunks.append(chunk)


# Build Devices mapping
devices_df = pd.DataFrame(sorted(unique_devices), columns=['Device'])
devices_df['DeviceID'] = range(1, len(devices_df) + 1)
devices_map = dict(zip(devices_df['Device'], devices_df['DeviceID']))

ad_events_chunks = []
for i, chunk in enumerate(processed_chunks, start=1):
    logger.info(
        f"Transforming chunk {transformed_count} out of {records_count}..."
    )

    ad_events_df = chunk[['EventID', 'CampaignName', 'UserID', 'Device', 'Timestamp',
                          'BidAmount', 'AdCost', 'WasClicked', 'ClickTimestamp', 'AdRevenue']]

    ad_events_df = ad_events_df.assign(
        CampaignID=ad_events_df['CampaignName'].replace(campaigns_map)
    ).drop(columns=['CampaignName'])

    ad_events_df = ad_events_df.assign(
        DeviceID=ad_events_df['Device'].replace(devices_map)
    ).drop(columns=['Device'])

    ad_events_chunks.append(ad_events_df)

# Step 4: Concatenate the final DataFrame
final_ad_events_df = pd.concat(ad_events_chunks, ignore_index=True)
final_ad_events_df.rename(columns={'EventID': 'AdEventID'}, inplace=True)

# --- Load ---
logger.info(f"Inserting data into DB...")
# region USERS TO SQL

genders_df.to_sql('Genders', engine, if_exists='append', index=False)
locations_df.to_sql('Locations', engine, if_exists='append', index=False)
interests_df.to_sql('Interests', engine, if_exists='append', index=False)
users_df.to_sql('Users', engine, if_exists='append',
                index=False, chunksize=chunk_size)
users_interests_df.to_sql('UsersInterests', engine,
                          if_exists='append', index=False, chunksize=chunk_size)

# endregion

# region CAMPAIGNS TO SQL

advertisers_df.to_sql('Advertisers', engine, if_exists='append', index=False)
ad_slots_df.to_sql('AdSlotSizes', engine, if_exists='append', index=False)
campaigns_df.to_sql('Campaigns', engine, if_exists='append', index=False)
campaigns_targeting_interests_df.to_sql(
    'CampaignsTargetingInterests', engine, if_exists='append', index=False)
campaigns_targeting_locations_df.to_sql(
    'CampaignsTargetingLocations', engine, if_exists='append', index=False)

# endregion

devices_df.to_sql('Devices', engine, if_exists='append', index=False)
final_ad_events_df.to_sql(
    'AdEvents', engine, if_exists='append', index=False)


logger.info(f"Data insertion into DB finished.")
