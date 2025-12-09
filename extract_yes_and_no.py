# read the csv file
import pandas as pd

data_file_path = './fake-smartcat-exps/amt-curation/real-turkers/dummy_exp/data/edi250_maverick_preprocessed_data.csv'
data_df = pd.read_csv(data_file_path)

# extract the yes and no tuples
yes_tuples = data_df[data_df['gold_label'] == 1.0]
no_tuples = data_df[data_df['gold_label'] == 0.0]

# write to csv
yes_tuples.to_csv(data_file_path.replace('.csv', '_yes.csv'), index=False)
no_tuples.to_csv(data_file_path.replace('.csv', '_no.csv'), index=False)

# print the number of yes and no tuples
print('Number of yes tuples: ', yes_tuples.shape[0])
print('Number of no tuples: ', no_tuples.shape[0])