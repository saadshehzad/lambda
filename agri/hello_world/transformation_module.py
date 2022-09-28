import pandas as pd
import time

# read_merge_farmer_data_S = time.time()
def read_merge_farmer_data(farmer_dataset):
    keep_farmer_columns = ['wheat_area_ha', 'wheat_production', 'wheat_yield', 'total_arable_ha',
            'crop_planted_area_ha', 'barley_area_ha', 'barley_production',
            'barley_yield', 'canola_area_ha', 'canola_production', 'canola_yield',
            'farm_km_to_2ndclosest_competitor_site',
            'farm_km_to_2ndclosest_grainflow', 'farm_km_to_3rdclosest_grainflow',
            'farm_km_to_closest_competitor_site', 'farm_km_to_closest_grainflow',
            'postcode', 'farm_size', 'grn']
    farmers_data = farmer_dataset[keep_farmer_columns]
    return farmers_data
# read_merge_farmer_data_E = time.time()
# print(read_merge_farmer_data_E - read_merge_farmer_data_S)
# print("........................")

# read_trades_data_S = time.time()
def read_trades_data(trades_dataset):
    keep_trades_columns=['product', 'quality', 'crop_year', 'delivery_period',
            'delivery_to_date', 'delivery_from_date', 'state', 'item_quantity',
            'delivery_state', 'fixed_price',
            'grower_number']
    trades = trades_dataset[keep_trades_columns]
    return trades
# read_trades_data_E = time.time()
# print(read_trades_data_E - read_trades_data_S)
# print("..........................")

# read_merge_trades_farmers_data_S = time.time()
def read_merge_trades_farmers_data(farmer_file, trade_file):
    farmers_data = read_merge_farmer_data(farmer_file)
    trades = read_trades_data(trade_file)
    trades_n_farmers = pd.merge(trades, farmers_data, left_on='grower_number', right_on='grn', how='inner').drop(columns=['grn'])
    trades_n_farmers['class']=1
    return trades_n_farmers
# read_merge_trades_farmers_data_E = time.time()
# print(read_merge_trades_farmers_data_E - read_merge_trades_farmers_data_S)
# print("..........................")


# def find_negative(df, current_crop_year='2016/2017'):
#     crop_years = list(df.crop_year.unique())
#     crop_years.sort()
#     years_to_process = crop_years[0:crop_years.index(current_crop_year)]
#     subset_df = df[df['crop_year'].isin(years_to_process)]
#     previous_years_farmers = set(subset_df.grower_number)
#     current_year_farmers = set(df[df['crop_year']==current_crop_year]['grower_number'])
#     missing_farmers_in_current = list(previous_years_farmers.difference(current_year_farmers))
#     negative_data = subset_df[subset_df['grower_number'].isin(missing_farmers_in_current)]
#     negative_data['class'] = 0
#     negative_data['crop_year'] = current_crop_year
#     negative_data.reset_index(drop=True, inplace=True)
#     return negative_data


# get_classification_data_S = time.time()
def get_classification_data(farmer_file, trade_file):
    df = read_merge_trades_farmers_data(farmer_file, trade_file)
    # crop_years = list(df.crop_year.unique())
    # crop_years.sort(reverse=True)
    # for year in crop_years:
    #     negative_data = find_negative(df, year)
    #     df = pd.concat([df, negative_data], axis=0)
    return df
# get_classification_data_E = time.time()
# print(get_classification_data_E - get_classification_data_S)
# print("..........................")