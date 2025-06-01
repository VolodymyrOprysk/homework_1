import pandas as pd
from sqlalchemy import create_engine, text

from db_schema_setup import create_db_tables

# --- Precreate tables ---
engine = create_engine(
    "mysql+pymysql://etl_user:etlpass@localhost:3306/etl_db"
)

create_db_tables()


# --- Extract ---
users_csv_df = pd.read_csv('data/users.csv')
campaigns_csv_df = pd.read_csv('data/campaigns.csv')


# --- Transform ---

# region Users CSV

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

# endregion

advertisers_df = pd.DataFrame(
    users_csv_df['Advertiser'].unique(), columns=['AdvertiserName'])
genders_df['AdvertiserID'] = range(1, len(advertisers_df) + 1)
genders_map = dict(
    zip(advertisers_df['AdvertiserName'], advertisers_df['GenderID']))

# --- Load ---
genders_df.to_sql('Genders', engine, if_exists='append', index=False)
locations_df.to_sql('Locations', engine, if_exists='append', index=False)
interests_df.to_sql('Interests', engine, if_exists='append', index=False)
users_df.to_sql('Users', engine, if_exists='append', index=False)
users_interests_df.to_sql('UsersInterests', engine,
                          if_exists='append', index=False)
