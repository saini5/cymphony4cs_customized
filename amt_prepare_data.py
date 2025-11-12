
from pathlib import Path
import csv
import pandas as pd
import random # Required for sampling with random_state

YES = 1.0
NO = 0.0

def prepare_data(exp_dir: Path, num_records_to_sample: int = 0, pre_filter_percentage: float = 0.1):
    """
    Prepare the data for the AMT curation experiment.
    Args:
        exp_dir: The directory of the experiment.
        num_records_to_sample: If > 0, samples this number of records (approximately 50-50 'Yes'/'No' gold_label) after initial filtering.
                               If 0, uses the entire dataset after initial filtering.
        pre_filter_percentage: The percentage of tuples to remove from the initial data to try to balance the distribution of gold_label values. Default is 0.1.
    Process:
    1. Load in the dataset from the given csv file.
        a. Assumes that the 'Match' column is already manually renamed to 'gold_label' column.
        b. Remove the rows where the 'gold_label' column entry does not exist or is empty.
    2. (Conditional) If num_records_to_sample > 0:
        a. Pick one tuple from a unique table_name (if column exists).
        b. From this, sample tuples to get an approximately 50-50% distribution of gold_label values ('Yes' and 'No').
        c. Limit the final sample to num_records_to_sample.
    3. Remove the columns gt_label (the gold expansion), and gold_label (whether the expansion is correct or not) from the processed dataset.
    4. Store the final preprocessed dataset in a new csv file.

    Returns:
        None
    """
    data_file_path = exp_dir / "data" / "edi250_maverick.csv"

    rows = []

    # read the csv using dictreader
    with open(data_file_path, "r") as f:
        reader = csv.DictReader(f)
        # remove the rows where the Match (gold_label) entry does not exist
        for row in reader:
            if row["gold_label"] is not None and row["gold_label"] != "":
                rows.append(row)
                # print(row) # Removed print for cleaner output

    print(f"Data is now filtered. Found {len(rows)} rows with non-empty 'gold_label'.")
    # write the rows to a new csv file (intermediate to retain gold_label for sampling)
    intermediate_file_path = data_file_path.with_name(data_file_path.name.replace(".csv", "_intermediate.csv"))
    with open(intermediate_file_path, "w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)  
    print("Intermediate data (with gold_label) saved to ", intermediate_file_path)

    # Read the intermediate data file as pandas dataframe for further processing
    df_initial = pd.read_csv(intermediate_file_path)
    print("Initial dataframe:")
    print(df_initial.head(10))
    print(df_initial.shape)
    print(df_initial.dtypes)
    df_for_final_processing = df_initial.copy() # This will hold the dataframe that goes to the final step

    # --- Conditional Sampling Logic (Steps 2a, 2b, 2c) ---
    if num_records_to_sample > 0:
        print()
        print(f"Attempting to sample {num_records_to_sample} records.")

        # Pre-filter: Ensure an approximately 50-50% distribution of gold_label values from the initial data
        # number of yes and no in the initial data
        num_yes = df_initial[df_initial['gold_label'] == YES].shape[0]
        num_no = df_initial[df_initial['gold_label'] == NO].shape[0]
        print(f"Number of yes in the initial data: {num_yes}")
        print(f"Number of no in the initial data: {num_no}")
        if num_yes > num_no:
            # remove 10% of the yes tuples
            df_initial_yes = df_initial[df_initial['gold_label'] == YES].sample(frac=1-pre_filter_percentage, random_state=42)
            df_initial_no = df_initial[df_initial['gold_label'] == NO]
            df_initial = pd.concat([df_initial_yes, df_initial_no])
            print(f"Removed {pre_filter_percentage*100}% of the yes tuples")
        else:
            # remove 10% of the no tuples
            df_initial_no = df_initial[df_initial['gold_label'] == NO].sample(frac=1-pre_filter_percentage, random_state=42)
            df_initial_yes = df_initial[df_initial['gold_label'] == YES]
            df_initial = pd.concat([df_initial_yes, df_initial_no])
            print(f"Removed {pre_filter_percentage*100}% of the no tuples")
        print("Pre-filtered initial dataframe:")
        print(df_initial.head(10))
        print(df_initial.shape)
        print(df_initial.dtypes)
        print(f"Number of yes in the pre-filtered initial data: {df_initial[df_initial['gold_label'] == YES].shape[0]}")
        print(f"Number of no in the pre-filtered initial data: {df_initial[df_initial['gold_label'] == NO].shape[0]}")

        # 2a. Pick one tuple from a unique table_name (if 'table_name' exists)
        sampled_per_table_df = pd.DataFrame() # Initialize empty dataframe
        if 'table_name' in df_initial.columns:
            print()
            # sampled_per_table_df = df_initial.groupby('table_name').first().reset_index()
            # random_state = random.randint(1, 10000)
            # print(f"Random state: {random_state}")
            sampled_per_table_df = df_initial.groupby('table_name').sample(n=1, random_state=42).reset_index(drop=True)
            print(f"Sampled one tuple per unique table_name. Resulting in {len(sampled_per_table_df)} records.")
        else:
            print()
            print("Warning: 'table_name' column not found for step 2a. Skipping sampling per table. Using all valid rows for balanced sampling.")
            sampled_per_table_df = df_initial.copy()
        
        print("Sampled per table dataframe:")
        print(sampled_per_table_df.head(10))
        print(sampled_per_table_df.shape)
        print(sampled_per_table_df.dtypes)

        # 2b & 2c. Sample to get approximately a 50-50% distribution of 'Yes' and 'No' gold_label values, up to num_records_to_sample
        if not sampled_per_table_df.empty and 'gold_label' in sampled_per_table_df.columns:
            print()
            df_yes = sampled_per_table_df[sampled_per_table_df['gold_label'] == YES]
            df_no = sampled_per_table_df[sampled_per_table_df['gold_label'] == NO]
            
            print("Yes dataframe: ", df_yes.shape)
            print("No dataframe: ", df_no.shape)

            # Determine how many 'Yes' and 'No' samples we need to try and get
            half_sample_target = num_records_to_sample // 2
            
            num_yes_to_take = min(half_sample_target, len(df_yes))
            num_no_to_take = min(half_sample_target, len(df_no))
            
            if num_yes_to_take > 0 or num_no_to_take > 0: # Proceed if we can take any samples
                print()
                sampled_yes = df_yes.sample(n=num_yes_to_take, random_state=42)
                sampled_no = df_no.sample(n=num_no_to_take, random_state=42)
                print("Sampled yes dataframe: ", sampled_yes.shape)
                print("Sampled no dataframe: ", sampled_no.shape)
                
                final_balanced_sample_df = pd.concat([sampled_yes, sampled_no])
                print("Final balanced sample dataframe: ", final_balanced_sample_df.shape)
                
                # Ensure we don't exceed num_records_to_sample if concatenation results in more
                if len(final_balanced_sample_df) > num_records_to_sample:
                    print()
                    print(f"Final sampled data has {len(final_balanced_sample_df)} records, which is more than the target {num_records_to_sample}. Sampling to {num_records_to_sample} records.")
                    final_balanced_sample_df = final_balanced_sample_df.sample(n=num_records_to_sample, random_state=42)

                df_for_final_processing = final_balanced_sample_df.sample(frac=1, random_state=42).reset_index(drop=True)
                print(f"Final sampled data has {len(df_for_final_processing)} records with approximate 50-50 'Yes'/'No' distribution (target {num_records_to_sample}).")
                
            else:
                print()
                print(f"Warning: Not enough 'Yes' or 'No' gold_label values for balanced sampling. Using a simple random sample up to {num_records_to_sample} records from available data.")
                # If balanced sampling isn't possible, just take num_records_to_sample from the grouped data
                if len(sampled_per_table_df) > num_records_to_sample:
                    print()
                    df_for_final_processing = sampled_per_table_df.sample(n=num_records_to_sample, random_state=42).reset_index(drop=True)
                else:
                    print()
                    df_for_final_processing = sampled_per_table_df.copy()
                print(f"Final sampled data (not balanced) has {len(df_for_final_processing)} records.")

        else:
            print()
            print(f"Warning: 'gold_label' column not found or intermediate sample is empty. Skipping sampling. Using original filtered data, potentially truncated to {num_records_to_sample}.")
            # If no gold_label or empty, just use original df_initial, possibly truncated
            if len(df_initial) > num_records_to_sample:
                print()
                df_for_final_processing = df_initial.sample(n=num_records_to_sample, random_state=42).reset_index(drop=True)
            else:
                print()
                df_for_final_processing = df_initial.copy()
            print(f"Final data (no balancing, potentially truncated) has {len(df_for_final_processing)} records.")
    else:
        print()
        print("num_records_to_sample is 0 or less, skipping sampling. Using full filtered dataset.")
    
    print("Dataframe for final processing:")
    print(df_for_final_processing.head(10))
    print(df_for_final_processing.shape)
    # number of yes and no in the dataframe for final processing
    print("Number of yes in the dataframe for final processing: ", df_for_final_processing[df_for_final_processing['gold_label'] == YES].shape[0])
    print("Number of no in the dataframe for final processing: ", df_for_final_processing[df_for_final_processing['gold_label'] == NO].shape[0])

    # Save this dataframe to another intermediate file with the name of the original file but with _intermediate_2.csv
    pre_final_file_path = intermediate_file_path.with_name(intermediate_file_path.name.replace(".csv", "_pre_final.csv"))
    df_for_final_processing.to_csv(pre_final_file_path, index=False)
    print("Pre-final (Final, but with gold_label) data saved to ", pre_final_file_path)
    
    # --- Final Column Removal and Saving (Steps 3 & 4 in docstring) ---
    columns_to_drop = []
    if 'gt_label' in df_for_final_processing.columns:
        columns_to_drop.append('gt_label')
    if 'gold_label' in df_for_final_processing.columns:
        columns_to_drop.append('gold_label')
        
    if columns_to_drop:
        print()
        df_final_processed = df_for_final_processing.drop(columns=columns_to_drop)
        print(f"Dropped columns: {', '.join(columns_to_drop)}.")
    else:
        print()
        df_final_processed = df_for_final_processing.copy()
        print("No 'gt_label' or 'gold_label' columns to drop.")
    
    print("Final dataframe:")
    print(df_final_processed.head(10))
    print(df_final_processed.shape)
    print(df_final_processed.dtypes)

    target_file_path = exp_dir / "data" / "edi250_maverick_preprocessed_data.csv"
    df_final_processed.to_csv(target_file_path, index=False)
    print("Data (final processed, potentially sampled) saved to ", target_file_path)


if __name__ == "__main__":
    # Example usage:
    from pathlib import Path
    # Create a dummy experiment directory and data file for testing
    dummy_exp_dir = Path("./fake-smartcat-exps/amt-curation/real-turkers/dummy_exp")
    dummy_exp_dir.mkdir(parents=True, exist_ok=True)
    (dummy_exp_dir / "data").mkdir(exist_ok=True)
    dummy_csv_path = dummy_exp_dir / "data" / "edi250_maverick.csv"

    if not dummy_csv_path.exists():
    
        # Create a dummy CSV with 'gold_label' and 'table_name' for testing
        dummy_data = [
            {'col1': 'a', 'gold_label': '1.0', 'table_name': 'T1'},
            {'col1': 'b', 'gold_label': '0.0', 'table_name': 'T1'},
            {'col1': 'c', 'gold_label': '1.0', 'table_name': 'T2'},
            {'col1': 'd', 'gold_label': '0.0', 'table_name': 'T2'},
            {'col1': 'e', 'gold_label': '1.0', 'table_name': 'T3'},
            {'col1': 'f', 'gold_label': '0.0', 'table_name': 'T3'},
            {'col1': 'g', 'gold_label': '1.0', 'table_name': 'T4'},
            {'col1': 'h', 'gold_label': '0.0', 'table_name': 'T4'},
            {'col1': 'i', 'gold_label': '', 'table_name': 'T5'},
            {'col1': 'j', 'gold_label': '1.0', 'table_name': 'T5'},
            {'col1': 'k', 'gold_label': '0.0', 'table_name': 'T6'},
        ]
        # with open(dummy_csv_path, 'w', newline='') as csvfile:
        #     fieldnames = dummy_data[0].keys()
        #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #     writer.writeheader()
        #     writer.writerows(dummy_data)

        # print("--- Testing prepare_data with sampling (num_records_to_sample=4) ---")
        # prepare_data(dummy_exp_dir, num_records_to_sample=4)
        # print("\n--- Testing prepare_data without sampling (num_records_to_sample=0) ---")
        # prepare_data(dummy_exp_dir, num_records_to_sample=0)
        # print("\n--- Testing prepare_data with sampling more than available (num_records_to_sample=100) ---")
        # prepare_data(dummy_exp_dir, num_records_to_sample=100)


        dummy_data.append({'col1': 'l', 'gold_label': '0.0', 'table_name': 'T7'})
        dummy_data.append({'col1': 'm', 'gold_label': '1.0', 'table_name': 'T7'})
        dummy_data.append({'col1': 'n', 'gold_label': '1.0', 'table_name': 'T8'})
        dummy_data.append({'col1': 'o', 'gold_label': '0.0', 'table_name': 'T8'})
        dummy_data.append({'col1': 'p', 'gold_label': '1.0', 'table_name': 'T9'})
        dummy_data.append({'col1': 'q', 'gold_label': '0.0', 'table_name': 'T9'})
        dummy_data.append({'col1': 'r', 'gold_label': '1.0', 'table_name': 'T10'})
        dummy_data.append({'col1': 's', 'gold_label': '0.0', 'table_name': 'T10'})
        with open(dummy_csv_path, 'w', newline='') as csvfile:
            fieldnames = dummy_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(dummy_data)
    
    num_records_to_sample = 100 # int(input("Enter the number of records to sample: "))
    pre_filter_percentage = 0.80

    print(f"--- Testing prepare_data with sampling (num_records_to_sample={num_records_to_sample}, pre_filter_percentage={pre_filter_percentage}) ---")
    prepare_data(dummy_exp_dir, num_records_to_sample, pre_filter_percentage)
    # Clean up dummy files/directory after tests (optional)
    # import shutil
    # shutil.rmtree(dummy_exp_dir)