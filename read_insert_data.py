import pandas as pd

import os
import json

from sqlalchemy import create_engine

# Creating connection to MySQL PhonePe database

# Connecting to MySQL database
user = ???
password = ???
host = ???

database = 'PhonePe'

engine = create_engine(
    url="mysql+pymysql://{0}:{1}@{2}/{3}".format(
        user, password, host, database
    )
)
custom_path ='C:\\Users\\Dell\\PycharmProjects'
# Fetch all the states and years data available
path = f'{custom_path}\\pulse\\data\\aggregated\\transaction\\country\\india\\state'
states = os.listdir(path)
path = f'{custom_path}\\pulse\\data\\aggregated\\transaction\\country\\india'
years = os.listdir(path)

years.remove('state')

# Fetching aggregated transaction data for all the years as dataframe and inserting into corresponding MySQL table<PhonePe.transactions>

transaction = []
for year in years:

    # %cd /content/pulse/data/aggregated/transaction/country/india/state/{state}/{year}

    directory = f'{custom_path}\\pulse\\data\\aggregated\\transaction\\country\\india\\{year}'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            # os.path.join(os.path.dirname(directory), filename)

            # Read the JSON file into a temporary dataframe
            file = f'{custom_path}\\pulse\\data\\aggregated\\transaction\\country\\india\\{year}\\{filename}'
            with open(file) as f:
                data = json.load(f)
                # pprint.pprint(data)
                for i in data['data']['transactionData']:
                    transaction.append({
                        'Year': year,
                        'Quarter': filename[0],
                        'transaction_category': i['name'],
                        'Transaction_count': i['paymentInstruments'][0]['count'],
                        'Transaction_Value': i['paymentInstruments'][0]['amount'],
                        'Transaction_type': i['paymentInstruments'][0]['type']
                    })

transaction_df = pd.DataFrame(transaction, columns=transaction[0].keys())
transaction_df.to_sql('transactions', con=engine, if_exists='append', index=False)

# Fetching Statewise aggregated transaction data for all the years as dataframe and inserting into corresponding MySQL table<PhonePe.state_transactions>

state_transaction = []
for state in states:
    for year in years:
        # %cd /content/pulse/data/aggregated/transaction/country/india/state/{state}/{year}
        directory = f'{custom_path}\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\{state}\\{year}'

        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                # os.path.join(os.path.dirname(directory), filename)
                file = f'{custom_path}\\pulse\\data\\aggregated\\transaction\\country\\india\\state\\{state}\\{year}\\{filename}'
                # Read the JSON file into a temporary dataframe
                with open(file) as f:
                    data = json.load(f)
                # pprint.pprint(data)
                for i in data['data']['transactionData']:
                    state_transaction.append({'state': state,
                                              'Year': year,
                                              'Quarter': filename[0],
                                              'transaction_category': i['name'],
                                              'Transaction_count': i['paymentInstruments'][0]['count'],
                                              'Transaction_Value': i['paymentInstruments'][0]['amount'],
                                              'Transaction_type': i['paymentInstruments'][0]['type']
                                              })

state_aggregated_transaction_df = pd.DataFrame(state_transaction, columns=state_transaction[0].keys())
state_aggregated_transaction_df.to_sql('state_transactions', con=engine, if_exists='append', index=False)

# Fetching Districtwise aggregated transaction data for all the years as dataframe and inserting into corresponding MySQL table<PhonePe.district_transactions>
district_transaction = []

for state in states:
    for year in years:

        # %cd / content / pulse / data / map / transaction / hover / country / india / state / {state} / {year}
        directory = f'{custom_path}\\pulse\\data\\map\\transaction\\hover\\country\\india\\state\\{state}\\{year}'
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file = f'{custom_path}\\pulse\\data\\map\\transaction\\hover\\country\\india\\state\\{state}\\{year}\\{filename}'
                # Read the JSON file into a temporary dataframe
                with open(file) as f:
                    data = json.load(f)

                    for i in data['data']['hoverDataList']:
                        district_transaction.append({'State': state,
                                                     'Year': year,
                                                     'Quarter': filename[0],
                                                     'District': i['name'],
                                                     'Transaction_type': i['metric'][0]['type'],
                                                     'Transaction_count': i['metric'][0]['count'],
                                                     'Transaction_Value': i['metric'][0]['amount']})

district_transaction_df = pd.DataFrame(district_transaction, columns=district_transaction[0].keys())
district_transaction_df.to_sql('district_transactions', con=engine, if_exists='append', index=False)

# Fetching to 10 states transaction data  as dataframe and inserting into corresponding MySQL table<PhonePe.top_state_transactions>

top_transaction_state = []

for year in years:
    #%cd / content / pulse / data / top / transaction / country / india / {year}
    directory = f'{custom_path}\\pulse\\data\\top\\transaction\\country\\india\\{year}'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file = f'{custom_path}\\pulse\\data\\top\\transaction\\country\\india\\{year}\\{filename}'
            # Read the JSON file into a temporary dataframe
            with open(file) as f:
                data = json.load(f)
                # pprint.pprint(data)
                for i in data['data']['states']:
                    top_transaction_state.append({'State': i['entityName'],
                                                  'Year': year,
                                                  'Quarter': filename[0],

                                                  'Transaction_type': i['metric']['type'],
                                                  'Transaction_count': i['metric']['count'],
                                                  'Transaction_Value': i['metric']['amount']})

top_transaction_state_df = pd.DataFrame(top_transaction_state, columns=top_transaction_state[0].keys())
top_transaction_state_df.to_sql('top_state_transactions', con=engine, if_exists='append', index=False)

# Fetching to 10 pincodes transaction data  as dataframe and inserting into corresponding MySQL table<PhonePe.top_pin_transactions>

top_transaction_pincodes = []

for year in years:
    #%cd / content / pulse / data / top / transaction / country / india / {year}
    directory = f'{custom_path}\\pulse\\data\\top\\transaction\\country\\india\\{year}'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file = f'{custom_path}\\pulse\\data\\top\\transaction\\country\\india\\{year}\\{filename}'
            # Read the JSON file into a temporary dataframe
            with open(file) as f:
                data = json.load(f)
                # pprint.pprint(data)
                for i in data['data']['pincodes']:
                    top_transaction_pincodes.append({'Pincode': i['entityName'],
                                                     'Year': year,
                                                     'Quarter': filename[0],
                                                     'Transaction_type': i['metric']['type'],
                                                     'Transaction_count': i['metric']['count'],
                                                     'Transaction_Value': i['metric']['amount']})

top_transaction_pincodes_df = pd.DataFrame(top_transaction_pincodes, columns=top_transaction_pincodes[0].keys())
top_transaction_pincodes_df.to_sql('top_pin_transactions', con=engine, if_exists='append', index=False)

# Fetching top 10 districts transaction data  as dataframe and inserting into corresponding MySQL table<PhonePe.top_district_transactions>

top_transaction_districts = []

for state in states:
    for year in years:
        #%cd / content / pulse / data / top / transaction / country / india / state / {state} / {year}
        directory = f'{custom_path}\\pulse\\data\\top\\transaction\\country\\india\\state\\{state}\\{year}'
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file = f'{custom_path}\\pulse\\data\\top\\transaction\\country\\india\\state\\{state}\\{year}\\{filename}'
                # Read the JSON file into a temporary dataframe
                with open(file) as f:
                    data = json.load(f)
                    # pprint.pprint(data)
                    for i in data['data']['districts']:
                        top_transaction_districts.append({'state': state,
                                                          'Year': year,
                                                          'Quarter': filename[0],
                                                          'District': i['entityName'],

                                                          'Transaction_type': i['metric']['type'],
                                                          'Transaction_count': i['metric']['count'],
                                                          'Transaction_Value': i['metric']['amount']})

top_transaction_districts_df = pd.DataFrame(top_transaction_districts, columns=top_transaction_districts[0].keys())
top_transaction_districts_df.to_sql('top_district_transactions', con=engine, if_exists='append', index=False)


#####################    USER INFORMATION ############################


# Fetching users data  as dataframe and inserting into corresponding MySQL table<PhonePe.users>
user_aggregated=[]

for year in years:
    directory=f'{custom_path}\\pulse\\data\\aggregated\\user\\country\\india\\{year}'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file = f'{custom_path}\\pulse\\data\\aggregated\\user\\country\\india\\{year}\\{filename}'
            # Read the JSON file into a temporary dataframe
            with open(file) as f:
                data = json.load(f)
                user_aggregated.append({'Year':year,
                                        'Quarter':filename[0],
                                        'Registered_Users':data['data']['aggregated']['registeredUsers'],
                                        'App_Opens':data['data']['aggregated']['appOpens']
                                        })
user_aggregate_df=pd.DataFrame(user_aggregated,columns=user_aggregated[0].keys())
user_aggregate_df.to_sql('users', con=engine, if_exists='append', index=False)

# Fetching statewise user data as dataframe and inserting into corresponding MySQL table<PhonePe.state_users>

state_users = []
for state in states:
    for year in years:
        #%cd / content / pulse / data / aggregated / user / country / india / {year}
        directory = f'{custom_path}\\pulse\\data\\aggregated\\user\\country\\india\\state\\{state}\\{year}'
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file = f'{custom_path}\\pulse\\data\\aggregated\\user\\country\\india\\state\\{state}\\{year}\\{filename}'
                # Read the JSON file into a temporary dataframe
                with open(file) as f:
                    data = json.load(f)
                    # pprint.pprint(data)
                    state_users.append({'state': state,
                                        'Year': year,
                                        'Quarter': filename[0],
                                        'Registered_Users': data['data']['aggregated']['registeredUsers'],
                                        'App_Opens': data['data']['aggregated']['appOpens']
                                        })

state_aggregated_user_df = pd.DataFrame(state_users, columns=state_users[0].keys())
state_aggregated_user_df.to_sql('state_users', con=engine, if_exists='append', index=False)

# Fetching districtwise user data as dataframe and inserting into MySQL table <PhonePe.district_user>

district_user=[]
for state in states:
    for year in years:

        #%cd / content / pulse / data / map / transaction / hover / country / india / state / {state} / {year}
        directory = f'{custom_path}\\pulse\\data\\map\\user\\hover\\country\\india\\state\\{state}\\{year}'
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file=f'{custom_path}\\pulse\\data\\map\\user\\hover\\country\\india\\state\\{state}\\{year}\\{filename}'
                # Read the JSON file into a temporary dataframe
                with open(file) as f:
                    data = json.load(f)
                    # pprint.pprint(data)
                    for district in data['data']['hoverData'].keys():
                        district_user.append({'State': state,
                                          'Year': year,
                                          'Quarter': filename[0],
                                          'District': district,
                                          'Registered_Users':data['data']['hoverData'][district]['registeredUsers'],
                                          'App_Opens':data['data']['hoverData'][district]['appOpens']
                                                        })

district_user_df = pd.DataFrame(district_user, columns=district_user[0].keys())
district_user_df.to_sql('district_users', con=engine, if_exists='append', index=False)

# Fetching top 10 state user data as dataframe and inserting into MySQL table <PhonePe.top_state_user>
top_users_state = []

for year in years:
    #%cd / content / pulse / data / top / transaction / country / india / {year}
    directory = f'{custom_path}\\pulse\\data\\top\\user\\country\\india\\{year}'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file=f'{custom_path}\\pulse\\data\\top\\user\\country\\india\\{year}\\{filename}'
            # Read the JSON file into a temporary dataframe
            with open(file) as f:
                data = json.load(f)
                # pprint.pprint(data)
                for i in data['data']['states']:
                    top_users_state.append({'State': i['name'],
                                                  'Year': year,
                                                  'Quarter': filename[0],

                                                  'Registered_Users': i['registeredUsers']
                                            })

top_users_state_df = pd.DataFrame(top_users_state, columns=top_users_state[0].keys())
top_users_state_df.to_sql('top_state_users', con=engine, if_exists='append', index=False)


# Fetching top 10 pincodes user data as dataframe and inserting into MySQL table <PhonePe.top_pin_user>
top_users_pincode = []

for year in years:
    #%cd / content / pulse / data / top / transaction / country / india / {year}
    directory = f'{custom_path}\\pulse\\data\\top\\user\\country\\india\\{year}'
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file=f'{custom_path}\\pulse\\data\\top\\user\\country\\india\\{year}\\{filename}'
            # Read the JSON file into a temporary dataframe
            with open(file) as f:
                data = json.load(f)
                # pprint.pprint(data)
                for i in data['data']['pincodes']:
                    top_users_pincode.append({'Pincode': i['name'],
                                                  'Year': year,
                                                  'Quarter': filename[0],

                                                  'Registered_Users': i['registeredUsers']
                                            })

top_users_pincode_df = pd.DataFrame(top_users_pincode, columns=top_users_pincode[0].keys())
top_users_pincode_df.to_sql('top_pin_users', con=engine, if_exists='append', index=False)

# Fetching top 10 districts user data as dataframe and inserting into MySQL table <PhonePe.top_district_user>

top_user_districts = []

for state in states:
    for year in years:
        #%cd / content / pulse / data / top / transaction / country / india / state / {state} / {year}
        directory = f'{custom_path}\\pulse\\data\\top\\user\\country\\india\\state\\{state}\\{year}'
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file=f'{custom_path}\\pulse\\data\\top\\user\\country\\india\\state\\{state}\\{year}\\{filename}'
                # Read the JSON file into a temporary dataframe
                with open(file) as f:
                    data = json.load(f)
                    # pprint.pprint(data)
                    for i in data['data']['districts']:
                        top_user_districts.append({'State': state,
                                                          'Year': year,
                                                          'Quarter': filename[0],
                                                          'District': i['name'],

                                                          'Registered_Users': i['registeredUsers']
                                                          })

top_user_districts_df = pd.DataFrame(top_user_districts, columns=top_user_districts[0].keys())
top_user_districts_df.to_sql('top_district_users', con=engine, if_exists='append', index=False)