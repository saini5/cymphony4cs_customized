# combine worker stats across exps
import pandas as pd
from pathlib import Path

# get all the stats_per_worker.csv files in the logs directory
exp4_pt2_dir = Path('./fake-smartcat-exps/amt-curation/real-turkers/exp4_part2_u12_p125_w124_r136_j404/')
exp4_pt1_dir = Path('./fake-smartcat-exps/amt-curation/real-turkers/exp4_part1_u12_p122_w121_r133_j395/')
exp3_dir = Path('./fake-smartcat-exps/amt-curation/real-turkers/exp3_u12_p119_w118_r130_j386/')

# read the stats_per_worker.csv files
exp4_pt2_stats = pd.read_csv(exp4_pt2_dir / 'logs' / 'stats_per_worker.csv')
# remove the precision column
exp4_pt2_stats = exp4_pt2_stats.drop(columns=['precision'])
exp4_pt1_stats = pd.read_csv(exp4_pt1_dir / 'logs' / 'stats_per_worker.csv')
# remove the precision column
exp4_pt1_stats = exp4_pt1_stats.drop(columns=['precision'])
exp3_stats = pd.read_csv(exp3_dir / 'logs' / 'stats_per_worker.csv')
# remove the precision column
exp3_stats = exp3_stats.drop(columns=['precision'])

# combine the stats_per_worker.csv files by joining across the worker_id column
combined_stats = exp4_pt2_stats.merge(exp4_pt1_stats, on='worker_id', how='outer').merge(exp3_stats, on='worker_id', how='outer')
# fill the missing values with 0
combined_stats = combined_stats.fillna(0)
print('Combined stats per worker: \n', combined_stats)
print('Combined stats per worker shape: \n', combined_stats.shape)
print('Combined stats per worker columns: \n', combined_stats.columns)

# sum the total_annotations_x, total_annotations_y, and total_annotations columns
combined_stats['total_annotations'] = combined_stats['total_annotations_x'] + combined_stats['total_annotations_y'] + combined_stats['total_annotations']
# sum the total_predictions_x, total_predictions_y, and total_predictions columns
combined_stats['total_predictions'] = combined_stats['total_predictions_x'] + combined_stats['total_predictions_y'] + combined_stats['total_predictions']
# sum the matches_x, matches_y, and matches columns
combined_stats['matches'] = combined_stats['matches_x'] + combined_stats['matches_y'] + combined_stats['matches']
# calculate the precision for each worker
combined_stats['precision'] = combined_stats['matches'] / combined_stats['total_predictions']
# sort the combined stats by precision descending
combined_stats = combined_stats.sort_values(by='precision', ascending=False)
print('Combined stats per worker sorted by precision descending: \n', combined_stats)

combined_stats = combined_stats[['worker_id', 'total_annotations', 'total_predictions', 'matches', 'precision']]
print('Combined stats per worker: \n', combined_stats)
print('Combined stats per worker shape: \n', combined_stats.shape)
print('Combined stats per worker columns: \n', combined_stats.columns)

# compute min, max, average annotations across workers
print('Minimum annotations across workers: \n', combined_stats['total_annotations'].min())
print('Maximum annotations across workers: \n', combined_stats['total_annotations'].max())
print('Average annotations across workers: \n', combined_stats['total_annotations'].mean())
# compute min, max, average predictions across workers
print('Minimum predictions across workers: \n', combined_stats['total_predictions'].min())
print('Maximum predictions across workers: \n', combined_stats['total_predictions'].max())
print('Average predictions across workers: \n', combined_stats['total_predictions'].mean())
# compute min, max, average matches across workers
print('Minimum matches across workers: \n', combined_stats['matches'].min())
print('Maximum matches across workers: \n', combined_stats['matches'].max())
print('Average matches across workers: \n', combined_stats['matches'].mean())
# compute min, max, average precision across workers
print('Minimum precision across workers: \n', combined_stats['precision'].min())
print('Maximum precision across workers: \n', combined_stats['precision'].max())
print('Average precision across workers: \n', combined_stats['precision'].mean())
print('Average precision by direct calculation: \n')
print('Total matches: \n', combined_stats['matches'].sum())
print('Total predictions: \n', combined_stats['total_predictions'].sum())
print('Average precision: \n')
print(combined_stats['matches'].sum() / combined_stats['total_predictions'].sum())


# save the combined stats to a csv file in the latest exp directory
combined_stats.to_csv(exp4_pt2_dir / 'logs' / 'combined_stats_per_worker.csv', index=True)