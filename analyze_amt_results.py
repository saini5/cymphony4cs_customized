from pathlib import Path
import csv
import pandas as pd
import json


def analyze_results(exp_dir):
    print("Analyzing results for experiment: ", exp_dir)
    print('AMT outputs: ')
    amt_outputs_file_path = exp_dir / "results" / "B_1"
    amt_outputs_df = pd.read_csv(amt_outputs_file_path)
    print(amt_outputs_df.head())
    
    print('Original data: ')
    # original_data_file_path = exp_dir / "results" / "id_vs_data_cymphony.csv"   # aka original data
    # original_data_df = pd.read_csv(original_data_file_path)
    # print(original_data_df.head())
    original_data_file_path = exp_dir / "results" / "ORIGINAL_DATA"   # aka original data
    original_data_df = pd.read_csv(original_data_file_path)
    if 'date_creation' in list(original_data_df.columns):
        original_data_df = original_data_df.drop(columns=['date_creation'])
    print(original_data_df.head())

    print("EDI data: ")
    edi_data_file_path = exp_dir / "data" / "edi250_maverick.csv"
    edi_data_df = pd.read_csv(edi_data_file_path)
    print(edi_data_df.head())

    # join the edi_data_df with the original_data_df on the id column to get id vs gold_label
    print('========= Statistics on Input data ==================')
    print("ID vs Gold Label:")
    id_vs_gold_label = edi_data_df.merge(original_data_df, on='id', how='inner')
    print(id_vs_gold_label.head())
    print(id_vs_gold_label.shape)
    # Total number of tuples with gold_label
    total_gold_labels = id_vs_gold_label.shape[0]
    print('Total tuples with gold_label: ', total_gold_labels)
    # Number of tuples with gold_label as 'Yes' and 'No'
    print('Number of tuples with gold_label as \'Yes\':')
    yes_tuples = id_vs_gold_label[id_vs_gold_label['gold_label'] == 1.0]
    print(yes_tuples.shape[0])
    print('Number of tuples with gold_label as \'No\':')
    no_tuples = id_vs_gold_label[id_vs_gold_label['gold_label'] == 0.0]
    print(no_tuples)
    # write to csv
    no_tuples.to_csv(exp_dir / 'results' / 'no_tuples.csv', index=False)
    print(no_tuples.shape[0])

    print('========= Statistics on Worker Votes ==================')
    # id vs annotation
    print("ID vs Annotation:")
    id_vs_annotation = amt_outputs_df.merge(original_data_df, on='_id', how='inner')
    print(id_vs_annotation.head())
    print(id_vs_annotation.shape)

    # SELECT COUNT(DISTINCT worker_id) FROM u1672_p107_w106_r118_j350_amt_outputs;
    print('Number of unique workers: ', len(amt_outputs_df['worker_id'].unique()))
    # SELECT COUNT(DISTINCT worker_id) FROM u1672_p107_w106_r118_j350_amt_outputs where annotation is not 'Cannot Determine'
    print('Number of unique workers with predictions: ', len(amt_outputs_df[amt_outputs_df['annotation'] != 'Cannot Determine']['worker_id'].unique()))

    # SELECT worker_id, count(annotation) as c 
	#   FROM public.u1672_p107_w106_r118_j350_amt_outputs 
	#   where annotation != 'Cannot Determine'
	# group by worker_id
    print('Number of annotations per worker:')
    annotations_per_worker = amt_outputs_df.groupby('worker_id').size().rename('total_annotations')
    print(annotations_per_worker)
    # Minimum, Maximum, and Average number of annotations per worker
    print('Minimum number of annotations per worker: ', annotations_per_worker.min())
    print('Maximum number of annotations per worker: ', annotations_per_worker.max())
    print('Average number of annotations per worker: ', annotations_per_worker.mean())
    print('Number of predictions per worker:')
    predictions_per_worker = amt_outputs_df[amt_outputs_df['annotation'] != 'Cannot Determine'].groupby('worker_id').size().rename('total_predictions')
    print(predictions_per_worker)
    # Minimum, Maximum, and Average number of predictions per worker
    print('Minimum number of predictions per worker: ', predictions_per_worker.min())
    print('Maximum number of predictions per worker: ', predictions_per_worker.max())
    print('Average number of predictions per worker: ', predictions_per_worker.mean())


    print('========= Statistics on Worker Precision ==================')

    # id vs annotation vs gold_label
    print("EDI data: ")
    edi_data_file_path = exp_dir / "data" / "edi250_maverick.csv"
    edi_data_df = pd.read_csv(edi_data_file_path)
    print(edi_data_df.head())

    print("ID vs Annotation vs Gold Label:")
    id_vs_annotation_vs_gold_label = id_vs_annotation.merge(edi_data_df, on='id', how='inner')
    print(id_vs_annotation_vs_gold_label.head())
    print(id_vs_annotation_vs_gold_label.shape)

    # # Deeper analysis
    # print('========= Deeper Analysis ==================')

    # # display for worker id AKSJ3C5O3V9RB
    # print('Tuples for worker id AKSJ3C5O3V9RB:')
    # # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    # print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'AKSJ3C5O3V9RB'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']])
    # # how many did this worker answer as yes, no, and cannot determine
    # print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'AKSJ3C5O3V9RB'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    # print('Number of no answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'AKSJ3C5O3V9RB'][id_vs_annotation_vs_gold_label['annotation'] == 'No'].shape[0])
    # print('Number of cannot determine answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'AKSJ3C5O3V9RB'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine'].shape[0])

    # # do the same for A26MRON9XGPVB5
    # print('Tuples for worker id A26MRON9XGPVB5:')
    # # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    # print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A26MRON9XGPVB5'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']])
    # # how many did this worker answer as yes, no, and cannot determine
    # print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A26MRON9XGPVB5'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    # print('Number of no answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A26MRON9XGPVB5'][id_vs_annotation_vs_gold_label['annotation'] == 'No'].shape[0])
    # print('Number of cannot determine answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A26MRON9XGPVB5'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine'].shape[0])

    # # A31Z5TPD8QKE26
    # print('Tuples for worker id A31Z5TPD8QKE26:')
    # # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    # print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A31Z5TPD8QKE26'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']].head(40))
    # # show those labeled incorrectly
    # print('Tuples labeled incorrectly by A31Z5TPD8QKE26:')
    # # gold label is 1.0 for Yes and 0.0 for No
    # print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A31Z5TPD8QKE26'][id_vs_annotation_vs_gold_label['gold_label'] == 1.0][id_vs_annotation_vs_gold_label['annotation'] != 'Yes'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']].head(40))
    # print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A31Z5TPD8QKE26'][id_vs_annotation_vs_gold_label['gold_label'] == 0.0][id_vs_annotation_vs_gold_label['annotation'] != 'No'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']].head(50))
    # # how many did this worker answer as yes, no, and cannot determine
    # print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A31Z5TPD8QKE26'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    # nos_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A31Z5TPD8QKE26'][id_vs_annotation_vs_gold_label['annotation'] == 'No']
    # print('No tuples for this worker: \n', nos_for_this_worker.head(40))
    # print('Number of no answers (with count): ', nos_for_this_worker.shape[0])
    # print('Number of cannot determine answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A31Z5TPD8QKE26'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine'].shape[0])

    # A2R2YZTSME1K3F
    print('Tuples for worker id A2R2YZTSME1K3F:')
    # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2R2YZTSME1K3F'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']].head(40))
    # show those labeled incorrectly
    print('Tuples labeled incorrectly by A2R2YZTSME1K3F:')
    # gold label is 1.0 for Yes and 0.0 for No
    print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2R2YZTSME1K3F'][id_vs_annotation_vs_gold_label['gold_label'] == 1.0][id_vs_annotation_vs_gold_label['annotation'] != 'Yes'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']].head(40))
    print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2R2YZTSME1K3F'][id_vs_annotation_vs_gold_label['gold_label'] == 0.0][id_vs_annotation_vs_gold_label['annotation'] != 'No'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']].head(50))
    # how many did this worker answer as yes, no, and cannot determine
    print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2R2YZTSME1K3F'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    nos_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2R2YZTSME1K3F'][id_vs_annotation_vs_gold_label['annotation'] == 'No']
    print('No tuples for this worker: \n', nos_for_this_worker.head(40))
    print('Number of no answers (with count): ', nos_for_this_worker.shape[0])
    print('Number of cannot determine answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2R2YZTSME1K3F'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine'].shape[0])

    # A2WGW5Y3ZFBDEC
    print('Tuples for worker id A2WGW5Y3ZFBDEC:')
    # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2WGW5Y3ZFBDEC'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']])
    # how many did this worker answer as yes, no, and cannot determine
    print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2WGW5Y3ZFBDEC'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    nos_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2WGW5Y3ZFBDEC'][id_vs_annotation_vs_gold_label['annotation'] == 'No']
    print('No tuples for this worker: \n', nos_for_this_worker.head(40))
    print('Number of no answers (with count): ', nos_for_this_worker.shape[0])
    cannot_determine_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A2WGW5Y3ZFBDEC'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine']
    print('Cannot determine tuples for this worker: \n', cannot_determine_for_this_worker.head(40))
    print('Number of cannot determine answers (with count): ', cannot_determine_for_this_worker.shape[0])
    
    # A149ROBL26JWPJ
    print('Tuples for worker id A149ROBL26JWPJ:')
    # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A149ROBL26JWPJ'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']])
    # how many did this worker answer as yes, no, and cannot determine
    print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A149ROBL26JWPJ'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    nos_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A149ROBL26JWPJ'][id_vs_annotation_vs_gold_label['annotation'] == 'No']
    print('No tuples for this worker: \n', nos_for_this_worker.head(40))
    print('Number of no answers (with count): ', nos_for_this_worker.shape[0])
    cannot_determine_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A149ROBL26JWPJ'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine']
    print('Cannot determine tuples for this worker: \n', cannot_determine_for_this_worker.head(40))
    print('Number of cannot determine answers (with count): ', cannot_determine_for_this_worker.shape[0])

    # A22KRF782ELLB0
    print('Tuples for worker id A22KRF782ELLB0:')
    # print only the id, worker_id, annotation, gold_label, column_name_x, expansion_x
    print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A22KRF782ELLB0'][['id', 'worker_id', 'annotation', 'gold_label', 'column_name_x', 'expansion_x']])
    # how many did this worker answer as yes, no, and cannot determine
    print('Number of yes answers: ', id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A22KRF782ELLB0'][id_vs_annotation_vs_gold_label['annotation'] == 'Yes'].shape[0])
    nos_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A22KRF782ELLB0'][id_vs_annotation_vs_gold_label['annotation'] == 'No']
    print('No tuples for this worker: \n', nos_for_this_worker.head(40))
    print('Number of no answers (with count): ', nos_for_this_worker.shape[0])
    cannot_determine_for_this_worker = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['worker_id'] == 'A22KRF782ELLB0'][id_vs_annotation_vs_gold_label['annotation'] == 'Cannot Determine']
    print('Cannot determine tuples for this worker: \n', cannot_determine_for_this_worker.head(40))
    print('Number of cannot determine answers (with count): ', cannot_determine_for_this_worker.shape[0])

    # # display for the ids 2499, 314, 2528, 3563
    # print('Tuples for ids 2499, 314, 2528, 356:')
    # # display only the id, worker_id, annotation
    # print(id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['id'].isin([2499, 314, 2528, 3563])][['id', 'worker_id', 'annotation']])

    # In the annotation column, replace 'Yes' with 1.0 and 'No' with 0.0, and 'Cannot Determine' with 0.5
    print('Replacing annotations with numerical values:')
    id_vs_annotation_vs_gold_label['annotation'] = id_vs_annotation_vs_gold_label['annotation'].map({'Yes': 1.0, 'No': 0.0, 'Cannot Determine': 0.5})
    print(id_vs_annotation_vs_gold_label.head())
    print(id_vs_annotation_vs_gold_label.shape)

    # Select only the columns 'worker_id', 'id', 'annotation', 'gold_label'
    print('Selecting only the columns worker_id, id, annotation, gold_label:')
    id_vs_annotation_vs_gold_label = id_vs_annotation_vs_gold_label[['worker_id', 'id', 'annotation', 'gold_label']]
    print(id_vs_annotation_vs_gold_label.head())
    print(id_vs_annotation_vs_gold_label.shape)
    
    # Create a copy of the dataframe for deeper analysis
    id_vs_annotation_vs_gold_label_for_deeper_analysis = id_vs_annotation_vs_gold_label.copy()

    # Display the tuples with annotation as 'Cannot Determine'
    print('Tuples with annotation as \'Cannot Determine\':')
    tuples_with_cannot_determine = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['annotation'] == 0.5]
    print(tuples_with_cannot_determine)
    print(tuples_with_cannot_determine.shape)

    # Remove the tuples with annotation as 'Cannot Determine' so they don't affect the precision calculation
    print('Removing tuples with annotation as \'Cannot Determine\':')
    id_vs_annotation_vs_gold_label = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['annotation'] != 0.5]
    print(id_vs_annotation_vs_gold_label.head())
    print(id_vs_annotation_vs_gold_label.shape)

    # select COUNT(*) as total_matches 
    # where AO.annotation = AT.gold_label
    total_matches = id_vs_annotation_vs_gold_label[id_vs_annotation_vs_gold_label['annotation'] == id_vs_annotation_vs_gold_label['gold_label']].shape[0]
    print('Total matches: ', total_matches)

    # SELECT COUNT(*) as total_annotations FROM amt_outputs
    total_predictions = id_vs_annotation_vs_gold_label.shape[0] 
    print('Total predictions: ', total_predictions)

    print('Average turker precision: ', total_matches / total_predictions) # This is the overall precision of the turkers.

    # Find the minimum, and maximum precision among turkers
    # 1. Find annotations per worker
    predictions_per_worker = id_vs_annotation_vs_gold_label.groupby('worker_id').size().rename('total_predictions')
    print('Predictions per worker: \n', predictions_per_worker)
    # 2. Find matches per worker
    matches_per_worker = id_vs_annotation_vs_gold_label[
        id_vs_annotation_vs_gold_label['annotation'] == id_vs_annotation_vs_gold_label['gold_label']
    ].groupby('worker_id').size().rename('matches')
    print('Matches per worker: \n', matches_per_worker)
    # 3. Merge and Calculate Precision
    # Merge the two Series on the worker_id index. Use fillna(0) in case a worker 
    # has 0 matches but did provide annotations (to avoid missing worker IDs).
    precision_per_worker = predictions_per_worker.to_frame().merge(
        matches_per_worker.to_frame(), 
        left_index=True, 
        right_index=True, 
        how='left'
    ).fillna(0) # Fill NaN matches with 0
    # Calculate Precision = Matches / Total Predictions
    precision_per_worker['precision'] = (
        precision_per_worker['matches'] / precision_per_worker['total_predictions']
    )

    # 4. Find Minimum, Maximum, and Sort
    min_precision = precision_per_worker['precision'].min()
    max_precision = precision_per_worker['precision'].max()

    # Sort the final result by precision descending
    precision_per_worker = precision_per_worker.sort_values(by='precision', ascending=False)

    print('\n--- Results Summary ---')
    print(f"Minimum Precision Found: {min_precision:.4f}")
    print(f"Maximum Precision Found: {max_precision:.4f}")
    print('\nPrecision per worker (Sorted):\n', precision_per_worker)

    # SELECT 
    #     (A.matches_per_worker * 1.0) / (B.annotations_per_worker) as precision, A.matches_per_worker, B.annotations_per_worker, A.worker_id, B.worker_id
    # FROM 
    #     (
    #         select COUNT(*) as matches_per_worker, D.worker_id as worker_id 
    #         FROM public.u12_p98_w97_r109_j326_drive_by_curation_votes D, public.u12_p98_w97_r109_j326_tuples T 
    #         where D.id = T.id and D.annotation = T.gold_label 
    #         GROUP BY D.worker_id
    #     ) A, 
    #     (SELECT COUNT(*) as annotations_per_worker, worker_id FROM public.u12_p98_w97_r109_j326_drive_by_curation_votes GROUP BY worker_id) B
    # WHERE A.worker_id = B.worker_id
    # ORDER BY precision DESC	-- 10/10, 5/10

    print('========= Statistics on Final Labels ==================')
    # Will analyze the final labels now

    print('final_labels.csv: ')
    final_labels_file_path = exp_dir / "results" / "final_labels.csv"
    final_labels_df = pd.read_csv(final_labels_file_path)
    if 'date_creation' in list(final_labels_df.columns):
        final_labels_df = final_labels_df.drop(columns=['date_creation'])
    print(final_labels_df.head())
    print(final_labels_df.shape)

    # ID vs Labels: (out of original data joined with final labels)
    print("ID vs Labels:")
    id_vs_labels_df = final_labels_df.merge(original_data_df, on='_id', how='inner')
    # id_vs_labels_file_path = exp_dir / "results" / "cymphony_data_vs_labels.csv"
    # id_vs_labels_df = pd.read_csv(id_vs_labels_file_path)
    print(id_vs_labels_df.head())
    print(id_vs_labels_df.shape)

    # we already have id vs gold_label from edi data
    print("EDI data: ")
    edi_data_file_path = exp_dir / "data" / "edi250_maverick.csv"
    edi_data_df = pd.read_csv(edi_data_file_path)
    print(edi_data_df.head())
    print(edi_data_df.shape)

    print("ID vs Label vs Gold Label:")
    id_vs_label_vs_gold_label = id_vs_labels_df.merge(edi_data_df, on='id', how='inner')
    print(id_vs_label_vs_gold_label.head())
    print(id_vs_label_vs_gold_label.shape)

    # In the label column, replace 'Yes' with 1.0 and 'No' with 0.0, and 'Cannot Determine' with 0.51 and 'undecided' with 0.52
    print('Replacing labels with numerical values:')
    id_vs_label_vs_gold_label['label'] = id_vs_label_vs_gold_label['label'].map({'Yes': 1.0, 'No': 0.0, 'Cannot Determine': 0.51, 'undecided': 0.52})
    print(id_vs_label_vs_gold_label.head())
    print(id_vs_label_vs_gold_label.shape)

    # Select only the columns 'id', 'label', 'gold_label'
    print('Selecting only the columns id, label, gold_label:')
    id_vs_label_vs_gold_label = id_vs_label_vs_gold_label[['id', 'label', 'gold_label']]
    print(id_vs_label_vs_gold_label.head())
    print(id_vs_label_vs_gold_label.shape)

    # DIsplay the number of tuples with label as 'Yes' and 'No'
    print('Number of tuples with label as \'Yes\':')
    yes_predictions = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] == 1.0].shape[0]
    print(yes_predictions)
    print('Number of tuples with label as \'No\':')
    no_predictions = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] == 0.0].shape[0]
    print(no_predictions)

    # Display the tuples with labels as 'Cannot Determine'
    print('Tuples with label as \'Cannot Determine\':')
    tuples_with_cannot_determine = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] == 0.51]
    print(tuples_with_cannot_determine)
    print(tuples_with_cannot_determine.shape)

    # Display the tuples with labels as 'undecided'
    print('Tuples with label as \'undecided\':')
    tuples_with_undecided = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] == 0.52]
    print(tuples_with_undecided)
    print(tuples_with_undecided.shape)

    edi_data_df_without_gold_label = edi_data_df.drop(columns=['gold_label'])
    edi_data_df_without_gold_label = edi_data_df_without_gold_label.rename(columns={'gt_label': 'gold_expansion'})
    id_vs_label_vs_gold_label = id_vs_label_vs_gold_label.merge(edi_data_df_without_gold_label, on='id', how='inner')
    id_vs_label_vs_gold_label = id_vs_label_vs_gold_label[['id', 'table_name', 'column_name', 'expansion', 'label', 'gold_label', 'gold_expansion']]
    
    print()
    print('ID vs Label vs Gold Label:')
    print(id_vs_label_vs_gold_label.head())
    print(id_vs_label_vs_gold_label.shape)
    
    # Display the following metrics:
    # Positive predictions that were actually positive (TP)
    tp = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 1.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ]
    print()
    print('TP tuples: \n', tp.head(20))
    print('TP count: ', tp.shape[0])

    # Positive predictions that were actually negative (FP)
    fp = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 1.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ]
    print()
    print('FP tuples: \n', fp.head(20))
    print('FP count: ', fp.shape[0])
    
    # Negative predictions that were actually positive (FN)
    fn = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ]
    print()
    print('FN tuples: \n', fn.head(20))
    print('FN count: ', fn.shape[0])
    
    # Negative predictions that were actually negative (TN)
    tn = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ]
    print()
    print('TN tuples: \n', tn.head(20))
    print('TN count: ', tn.shape[0])

    # Cannot determine predictions that were actually positive
    cd_but_positive = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.51) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ]
    print()
    print('Tuples of Cannot determine predictions that were actually positive: \n', cd_but_positive.head(20))
    print('Count of Cannot determine predictions that were actually positive: ', cd_but_positive.shape[0])
    
    # Cannot determine predictions that were actually negative
    cd_but_negative = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.51) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ]
    print()
    print('Tuples of Cannot determine predictions that were actually negative: \n', cd_but_negative.head(20))
    print('Count of Cannot determine predictions that were actually negative: ', cd_but_negative.shape[0])

    # undecided predictions that were actually positive
    undecided_but_positive = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.52) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ]
    print()
    print('Tuples of Undecided predictions that were actually positive: \n', undecided_but_positive.head(20))
    print('Count of Undecided predictions that were actually positive: ', undecided_but_positive.shape[0])
    
    # undecided predictions that were actually negative
    undecided_but_negative = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.52) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ]
    print()
    print('Tuples of Undecided predictions that were actually negative: \n', undecided_but_negative.head(20))
    print('Count of Undecided predictions that were actually negative: ', undecided_but_negative.shape[0])

    # complete set of gold yes
    id_vs_label_vs_gold_label_yes = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['gold_label'] == 1.0]
    print()
    print('Complete set of gold yes: \n', id_vs_label_vs_gold_label_yes)
    print('Count of complete set of gold yes: ', id_vs_label_vs_gold_label_yes.shape[0])
    # save to csv
    id_vs_label_vs_gold_label_yes.to_csv(exp_dir / 'results' / 'id_vs_label_vs_gold_label_yes.csv', index=False)
    
    # complete set of gold no
    id_vs_label_vs_gold_label_no = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['gold_label'] == 0.0]
    print()
    print('Complete set of gold no: \n', id_vs_label_vs_gold_label_no)
    print('Count of complete set of gold no: ', id_vs_label_vs_gold_label_no.shape[0])
    # save to csv
    id_vs_label_vs_gold_label_no.to_csv(exp_dir / 'results' / 'id_vs_label_vs_gold_label_no.csv', index=False)

    print()
    total_input_tuples = original_data_df.shape[0]
    print('Total input tuples: ', total_input_tuples)
    total_final_labels = id_vs_label_vs_gold_label.shape[0]
    print('Total final labels: ', total_final_labels)

    # Create a copy of the dataframe for deeper analysis
    id_vs_label_vs_gold_label_for_deeper_analysis = id_vs_label_vs_gold_label.copy()

    # Calculate Precision

    # Remove the tuples with labels as 'Cannot Determine' so they don't affect the precision calculation
    print()
    print('Removing tuples with labels as \'Cannot Determine\':')
    id_vs_label_vs_gold_label = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] != 0.51]
    print(id_vs_label_vs_gold_label.head())
    print(id_vs_label_vs_gold_label.shape)

    # Remove the tuples with labels as 'undecided' so they don't affect the precision calculation
    print('Removing tuples with labels as \'undecided\':')
    id_vs_label_vs_gold_label = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] != 0.52]
    print(id_vs_label_vs_gold_label.head())
    print(id_vs_label_vs_gold_label.shape)


    # select COUNT(*) as total_matches 
    # where AF.label = AT.gold_label and label is not 'Cannot Determine' and label is not 'undecided'
    correct_predictions = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['label'] == id_vs_label_vs_gold_label['gold_label']].shape[0]
    print('Correct predictions: ', correct_predictions)
    # SELECT COUNT(*) as predictions FROM amt_final_labels where label is not 'Cannot Determine' and label is not 'undecided'
    predictions = id_vs_label_vs_gold_label.shape[0] 
    print('Predictions: ', predictions)
    print('Precision: ', correct_predictions / predictions)

    # Calculate Recall = number of correct predictions / Total number of input tuples or final labels
    recall = correct_predictions / total_input_tuples
    print('Recall: ', recall)

    # Deeper analysis
    print('========= Deeper Analysis ==================')

    # 1. Prepare the data for deeper analysis
    # remove gold label
    id_vs_annotation = id_vs_annotation_vs_gold_label_for_deeper_analysis.drop(columns=['gold_label'])  # annotation column could contain 1.0, 0.0, 0.5 (cannot determine)
    # make the annotations more readable by converting 1.0 to 'Y', 0.0 to 'N', and 0.5 to 'C'
    # id_vs_annotation['annotation'] = id_vs_annotation['annotation'].map({1.0: 'Y', 0.0: 'N', 0.5: 'C'})
    
    id_vs_label_vs_gold_label = id_vs_label_vs_gold_label_for_deeper_analysis   # label column could contain 1.0, 0.0, 0.51 (cannot determine), 0.52 (undecided)
    
    # 2. Analyze the data for deeper analysis
    # 2. 1 what are the gold nos?
    gold_nos = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['gold_label'] == 0.0]
    # 2.1.1 out of these gold nos, which were labeled as yes by turkers?
    gold_nos_labeled_as_yes = gold_nos[gold_nos['label'] == 1.0]
    # 2.1.1.1 out of these gold nos that got labeled as yes, what is the annotation breakdown by turkers? For example, YYY, YYC, YYN? we might need to join with id_vs_annotation to get the annotations.
    gold_nos_labeled_as_yes_annotations = gold_nos_labeled_as_yes.merge(id_vs_annotation, on='id', how='inner')
    print('Gold nos labeled as yes annotations:')
    print(gold_nos_labeled_as_yes_annotations.head(10))
    print(gold_nos_labeled_as_yes_annotations.shape)
    # for each id, list the annotations by turkers. example, 1, <1.0, 1.0, 1.0> such that the contents of the list are in decreasing order.
    gold_nos_labeled_as_yes_annotations = gold_nos_labeled_as_yes_annotations.groupby('id').agg({'annotation': lambda x: sorted(list(x), reverse=True)})
    print('Gold nos labeled as yes annotations breakdown (grouped by id):')
    print(gold_nos_labeled_as_yes_annotations.head(10))
    print(gold_nos_labeled_as_yes_annotations.shape)
    # count the number of times each annotation combination occurs
    gold_nos_labeled_as_yes_annotations['annotation_tuple'] = gold_nos_labeled_as_yes_annotations['annotation'].apply(tuple)
    print('Gold nos labeled as yes annotations tuple:')
    print(gold_nos_labeled_as_yes_annotations['annotation_tuple'].head(10))
    print(gold_nos_labeled_as_yes_annotations['annotation_tuple'].shape)
    gold_nos_labeled_as_yes_annotations_count = gold_nos_labeled_as_yes_annotations.groupby('annotation_tuple').size().rename('count')
    print('Gold nos labeled as yes annotations breakdown (counted by annotation tuple):')
    print(gold_nos_labeled_as_yes_annotations_count.head(10))
    print(gold_nos_labeled_as_yes_annotations_count.shape)
    
    # 2.1.2 out of these gold nos, which were labeled as NOT yes by turkers?
    gold_nos_labeled_as_not_yes = gold_nos[gold_nos['label'] != 1.0]    # this means gold_nos['label'] is 0.0 or 0.51 or 0.52
    # 2.1.2.1 out of these gold nos that got labeled as not yes, what is the annotation breakdown by turkers? For example, NNN, NNC, NNY? we need to join with id_vs_annotation to get the annotations.
    gold_nos_labeled_as_not_yes_annotations = gold_nos_labeled_as_not_yes.merge(id_vs_annotation, on='id', how='inner')
    print('Gold nos labeled as not yes annotations:')
    print(gold_nos_labeled_as_not_yes_annotations.head(45))
    print(gold_nos_labeled_as_not_yes_annotations.shape)
    # for each id, list the annotations by turkers. example, 1, <0.0, 0.0, 0.0> such that the contents of the list are in decreasing order.
    gold_nos_labeled_as_not_yes_annotations = gold_nos_labeled_as_not_yes_annotations.groupby('id').agg({'annotation': lambda x: sorted(list(x), reverse=True)})
    print('Gold nos labeled as not yes annotations breakdown (grouped by id):')
    print(gold_nos_labeled_as_not_yes_annotations.head(10))
    print(gold_nos_labeled_as_not_yes_annotations.shape)
    # count the number of times each annotation combination occurs
    gold_nos_labeled_as_not_yes_annotations['annotation_tuple'] = gold_nos_labeled_as_not_yes_annotations['annotation'].apply(tuple)
    print('Gold nos labeled as not yes annotations tuple:')
    print(gold_nos_labeled_as_not_yes_annotations['annotation_tuple'].head(10))
    print(gold_nos_labeled_as_not_yes_annotations['annotation_tuple'].shape)
    gold_nos_labeled_as_not_yes_annotations_count = gold_nos_labeled_as_not_yes_annotations.groupby('annotation_tuple').size().rename('count')
    print('Gold nos labeled as not yes annotations breakdown (counted by annotation tuple):')
    print(gold_nos_labeled_as_not_yes_annotations_count.head(10))
    print(gold_nos_labeled_as_not_yes_annotations_count.shape)

    # 2. 1 what are the gold yes?
    gold_yes = id_vs_label_vs_gold_label[id_vs_label_vs_gold_label['gold_label'] == 1.0]
    # 2.1.1 out of these gold yes, which were labeled as yes by turkers?
    gold_yes_labeled_as_yes = gold_yes[gold_yes['label'] == 1.0]
    # 2.1.1.1 out of these gold yes that got labeled as yes, what is the annotation breakdown by turkers? For example, YYY, YYC, YYN? we need to join with id_vs_annotation to get the annotations.
    gold_yes_labeled_as_yes_annotations = gold_yes_labeled_as_yes.merge(id_vs_annotation, on='id', how='inner')
    print('Gold yes labeled as yes annotations:')
    print(gold_yes_labeled_as_yes_annotations.head(10))
    print(gold_yes_labeled_as_yes_annotations.shape)
    # for each id, list the annotations by turkers. example, 1, <1.0, 1.0, 1.0> such that the contents of the list are in decreasing order.
    gold_yes_labeled_as_yes_annotations = gold_yes_labeled_as_yes_annotations.groupby('id').agg({'annotation': lambda x: sorted(list(x), reverse=True)})
    print('Gold yes labeled as yes annotations breakdown (grouped by id):')
    print(gold_yes_labeled_as_yes_annotations.head(10))
    print(gold_yes_labeled_as_yes_annotations.shape)
    # count the number of times each annotation combination occurs
    gold_yes_labeled_as_yes_annotations['annotation_tuple'] = gold_yes_labeled_as_yes_annotations['annotation'].apply(tuple)
    print('Gold yes labeled as yes annotations tuple:')
    print(gold_yes_labeled_as_yes_annotations['annotation_tuple'].head(10))
    print(gold_yes_labeled_as_yes_annotations['annotation_tuple'].shape)
    gold_yes_labeled_as_yes_annotations_count = gold_yes_labeled_as_yes_annotations.groupby('annotation_tuple').size().rename('count')
    print('Gold yes labeled as yes annotations breakdown (counted by annotation tuple):')
    print(gold_yes_labeled_as_yes_annotations_count.head(10))
    print(gold_yes_labeled_as_yes_annotations_count.shape)

if __name__ == "__main__":
    exp_dir = Path('./fake-smartcat-exps/amt-curation/real-turkers/dummy_exp/')
    analyze_results(exp_dir)