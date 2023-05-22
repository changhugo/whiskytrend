import subprocess
import json
import pandas as pd
import codecs

#Distillery INFO
url = "https://whiskyhunter.net/api/distilleries_info/"
csrf_token = "IRsQbjpb6n1vhrxdMsbXrZa5jUjXNrdg1aVnvQEjjKTSEpwYbIjGD7TMmqpfQMXr"


# Set up the cURL command
curl_command = "curl -X 'GET' \
  '{}' \
  -H 'accept: application/json' \
  -H 'X-CSRFToken: {}'".format(url,csrf_token)

# Call the cURL command using subprocess
json_data = subprocess.check_output(curl_command, shell=True)

parsed_json = json.loads(json_data)

# Convert the JSON data to a DataFrame
df_distilleries_info = pd.json_normalize(parsed_json)
#df_distilleries_info = df_distilleries_info.set_index('slug')

#Only run when file is not found
df_distilleries_info.to_csv('distilleries_info.csv')

"""
# read from csv
df_distilleries_info_csv = pd.read_csv('distilleries_info.csv')
df_distilleries_info_csv = df_distilleries_info_csv.drop(df_distilleries_info_csv.columns[0], axis=1)
merged_df_distilleries_info = pd.merge(df_distilleries_info, df_distilleries_info_csv, on='slug', how='outer')

merged_df_distilleries_info['name'] = merged_df_distilleries_info['name_x'].combine_first(merged_df_distilleries_info['name_y'])
merged_df_distilleries_info = merged_df_distilleries_info.drop(['name_x', 'name_y'], axis=1)
merged_df_distilleries_info['country'] = merged_df_distilleries_info['country_x'].combine_first(merged_df_distilleries_info['country_y'])
merged_df_distilleries_info = merged_df_distilleries_info.drop(['country_x', 'country_y'], axis=1)
merged_df_distilleries_info['whiskybase_whiskies'] = merged_df_distilleries_info['whiskybase_whiskies_x'].combine_first(merged_df_distilleries_info['whiskybase_whiskies_y'])
merged_df_distilleries_info = merged_df_distilleries_info.drop(['whiskybase_whiskies_x', 'whiskybase_whiskies_y'], axis=1)
merged_df_distilleries_info['whiskybase_votes'] = merged_df_distilleries_info['whiskybase_votes_x'].combine_first(merged_df_distilleries_info['whiskybase_votes_y'])
merged_df_distilleries_info = merged_df_distilleries_info.drop(['whiskybase_votes_x', 'whiskybase_votes_y'], axis=1)
merged_df_distilleries_info['whiskybase_rating'] = merged_df_distilleries_info['whiskybase_rating_x'].combine_first(merged_df_distilleries_info['whiskybase_rating_y'])
merged_df_distilleries_info = merged_df_distilleries_info.drop(['whiskybase_rating_x', 'whiskybase_rating_y'], axis=1)

merged_df_distilleries_info.to_csv('distilleries_info.csv')
"""



##Auction data
#Distillery INFO
url = "https://whiskyhunter.net/api/auctions_data/"
csrf_token = "IRsQbjpb6n1vhrxdMsbXrZa5jUjXNrdg1aVnvQEjjKTSEpwYbIjGD7TMmqpfQMXr"

# Set up the cURL command
curl_command = "curl -X 'GET' \
  '{}' \
  -H 'accept: application/json' \
  -H 'X-CSRFToken: {}'".format(url,csrf_token)

# Call the cURL command using subprocess
json_data = subprocess.check_output(curl_command, shell=True)

parsed_json = json.loads(json_data)

# Convert the JSON data to a DataFrame
df_auctions_data = pd.json_normalize(parsed_json)
df_auctions_data = df_auctions_data.set_index('auction_slug')
df_auctions_data.to_csv('acutions_data.csv')


# Distillery Data summary
# Print the DataFrame
df_distilleries_info_csv = pd.read_csv('distilleries_info.csv')
distilleries_array = list(set(df_distilleries_info_csv.iloc[:, 1].values))

#distillery = "yoishi"

# Loop through the array and print each value
for distillery in distilleries_array:
    print (distillery +" PROCESSING")
    if ("'" not in distillery ):
        df_distilleries_summary = pd.read_csv('distilleries_summary.csv')
        df_distilleries_summary = df_distilleries_summary.drop(df_distilleries_summary.columns[0], axis=1)
        curl_command = "curl -X 'GET' \
          '{}' \
          -H 'accept: application/json' \
          -H 'X-CSRFToken: {}'".format('https://whiskyhunter.net/api/distillery_data/'+distillery+'/',csrf_token)

        json_data = subprocess.check_output(curl_command, shell=True)

        if (json_data.decode("utf-8") != '' and "Error 404" not in json_data.decode("utf-8")):
            parsed_json = json.loads(json_data)

            current_df = pd.json_normalize(parsed_json)
            current_df['dt'] = pd.to_datetime(current_df['dt'], format='%Y-%m-%d').dt.strftime('%-m/%-d/%y')
            #print(current_df)
            df_merged_summary = pd.merge(current_df,df_distilleries_summary,on=['dt', 'slug','name'],how='outer')

            df_merged_summary['winning_bid_max'] = df_merged_summary['winning_bid_max_x'].combine_first(df_merged_summary['winning_bid_max_y'])
            df_merged_summary = df_merged_summary.drop(['winning_bid_max_x', 'winning_bid_max_y'], axis=1)
            df_merged_summary['winning_bid_min'] = df_merged_summary['winning_bid_min_x'].combine_first(df_merged_summary['winning_bid_min_y'])
            df_merged_summary = df_merged_summary.drop(['winning_bid_min_x', 'winning_bid_min_y'], axis=1)
            df_merged_summary['winning_bid_mean'] = df_merged_summary['winning_bid_mean_x'].combine_first(df_merged_summary['winning_bid_mean_y'])
            df_merged_summary = df_merged_summary.drop(['winning_bid_mean_x', 'winning_bid_mean_y'], axis=1)
            df_merged_summary['trading_volume'] = df_merged_summary['trading_volume_x'].combine_first(df_merged_summary['trading_volume_y'])
            df_merged_summary = df_merged_summary.drop(['trading_volume_x', 'trading_volume_y'], axis=1)
            df_merged_summary['lots_count'] = df_merged_summary['lots_count_x'].combine_first(df_merged_summary['lots_count_y'])
            df_merged_summary = df_merged_summary.drop(['lots_count_x', 'lots_count_y'], axis=1)
            
            df_distilleries_summary = df_merged_summary
            df_distilleries_summary.to_csv('distilleries_summary.csv')
        else:
            print (distillery +" NO DATA or ERROR")
    else:
        print (distillery +" name ERROR")

#df_distilleries_summary.to_csv('distilleries_summary.csv')

