from pathlib import Path
import csv
import pandas as pd
import json


def analyze_results(exp_dir):
    print("Analyzing results for experiment: ", exp_dir)
    print('AMT outputs: ')
    amt_outputs_file_path = exp_dir / "results" / "B_1.csv"
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
    yes_tuples = id_vs_gold_label[id_vs_gold_label['gold_label'] == 1.0].shape[0]
    print(yes_tuples)
    print('Number of tuples with gold_label as \'No\':')
    no_tuples = id_vs_gold_label[id_vs_gold_label['gold_label'] == 0.0].shape[0]
    print(no_tuples)

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

    # Display the following metrics:
    # Positive predictions that were actually positive (TP)
    tp = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 1.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ].shape[0]
    print('TP: ', tp)
    # Positive predictions that were actually negative (FP)
    fp = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 1.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ].shape[0]
    print('FP: ', fp)
    # Negative predictions that were actually positive (FN)
    fn = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ].shape[0]
    print('FN: ', fn)
    # Negative predictions that were actually negative (TN)
    tn = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.0) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ].shape[0]
    print('TN: ', tn)
    # Cannot determine predictions that were actually positive
    cd_but_positive = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.51) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ].shape[0]
    print('Cannot determine predictions that were actually positive: ', cd_but_positive)
    # Cannot determine predictions that were actually negative
    cd_but_negative = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.51) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ].shape[0]
    print('Cannot determine predictions that were actually negative: ', cd_but_negative)
    # undecided predictions that were actually positive
    undecided_but_positive = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.52) & 
        (id_vs_label_vs_gold_label['gold_label'] == 1.0)
    ].shape[0]
    print('Undecided predictions that were actually positive: ', undecided_but_positive)
    # undecided predictions that were actually negative
    undecided_but_negative = id_vs_label_vs_gold_label[
        (id_vs_label_vs_gold_label['label'] == 0.52) & 
        (id_vs_label_vs_gold_label['gold_label'] == 0.0)
    ].shape[0]
    print('Undecided predictions that were actually negative: ', undecided_but_negative)

    total_input_tuples = original_data_df.shape[0]
    print('Total input tuples: ', total_input_tuples)
    total_final_labels = id_vs_label_vs_gold_label.shape[0]
    print('Total final labels: ', total_final_labels)

    # Calculate Precision

    # Remove the tuples with labels as 'Cannot Determine' so they don't affect the precision calculation
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



    # # original_data_df = original_data_df.head(70)
    # # final_labels_df = final_labels_df.head(70)
    # # print(original_data_df)
    # # print(final_labels_df)

    # # join the final_labels_df with the original_data_df on the _id column
    # final_labels_df = final_labels_df.merge(original_data_df, on='_id', how='inner')
    # print(final_labels_df.head())
    # # print(final_labels_df)
    # print(final_labels_df.shape)
    # # get the accuracy of the labels by comparing with the Match (gold_label) column
    # numerator = sum(final_labels_df['gold_label'] == final_labels_df['label'])
    # denominator = len(final_labels_df)
    # accuracy = numerator / denominator
    # print(f'Accuracy = {numerator} / {denominator} = {accuracy}')
    # accuracy = accuracy_score(final_labels_df['gold_label'], final_labels_df['label'])
    # print('Accuracy: ', accuracy)
    # # precision = precision_score(final_labels_df['gold_label'], final_labels_df['label'])
    # # print('Precision: ', precision)
    # # recall = recall_score(final_labels_df['gold_label'], final_labels_df['label'])
    # # print('Recall: ', recall)
    # # f1 = f1_score(final_labels_df['gold_label'], final_labels_df['label'])
    # # print('F1 Score: ', f1)
    # cm = confusion_matrix(final_labels_df['gold_label'], final_labels_df['label'], labels=[0.0, 1.0])
    # print('Confusion Matrix: ', cm)

    # TN, FP, FN, TP = cm.ravel()
    # print('TN: ', TN)
    # print('FP: ', FP)
    # print('FN: ', FN)
    # print('TP: ', TP)

    # # show those rows where final_labels_df gold_label is not equal to label
    # failures = final_labels_df[final_labels_df['gold_label'] != final_labels_df['label']]
    # print('All failures: ', failures.shape)
    # print(failures.head())
    # print(failures)
    # failures_fp = failures[failures['gold_label'] == 0.0]
    # print('FP failures: ', failures_fp.shape)
    # print(failures_fp.head())
    # failures_fn = failures[failures['gold_label'] == 1.0]
    # print('FN failures: ', failures_fn.shape)
    # print(failures_fn.head())


if __name__ == "__main__":
    analyze_results(Path("exp_u1672_p107_w106_r118"))