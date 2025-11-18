
from pathlib import Path
import csv
import pandas as pd
import random # Required for sampling with random_state

YES = 1.0
NO = 0.0

def prepare_data_chunks(exp_dir: Path, chunk_size: int = 500):
    """
    Prepare the data for the AMT curation experiment.
    Args:
        exp_dir: The directory of the experiment.
        chunk_size: The size of the chunks to process. Default is 500.
    Process:
    1. Load in the dataset from the given csv file.
        a. Assumes that the 'Match' column is already manually renamed to 'gold_label' column.
        b. Remove the rows where the 'gold_label' column entry does not exist or is empty.
    2. 2918 records remain with yes: 2508, no: 410
        a. shuffle the rows 
        b. save to edi250_maverick_preprocessed_full_data.csv
    3. Start chunking:
        a. pick rows 1-500
        d. calculate number of yes and no in the chunk
        e. display the first 20 records
        f. remove the gold columns ‘gt_label’ and ‘gold_label’
        g. save to edi250_maverick_preprocessed_1.csv
        h. repeat for the next chunks i.e. 501-1000, 1001-1500, 1501-2000, 2001-2500, 2501-2918
            pick rows 501-1000
            yes: , no: 
            display the first 20 records
            remove the gold columns ‘gt_label’ and ‘gold_label’
            save to edi250_maverick_preprocessed_2.csv
            pick rows 1001-1500
            yes: , no: 
            display the first 20 records
            remove the gold columns ‘gt_label’ and ‘gold_label’
            save to edi250_maverick_preprocessed_3.csv
            pick rows 1501-2000
            yes: , no: 
            display the first 20 records
            remove the gold columns ‘gt_label’ and ‘gold_label’
            save to edi250_maverick_preprocessed_4.csv
            pick rows 2001-2500
            yes: , no: 
            display the first 20 records
            remove the gold columns ‘gt_label’ and ‘gold_label’
            save to edi250_maverick_preprocessed_5.csv
            pick rows 2501-2918
            yes: , no: 
            display the first 20 records
            remove the gold columns ‘gt_label’ and ‘gold_label’
            save to edi250_maverick_preprocessed_6.csv

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
    intermediate_file_path = data_file_path.with_name(data_file_path.name.replace(".csv", "_preprocessed_full_data_intermediate.csv"))
    with open(intermediate_file_path, "w", encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames, lineterminator='\n')
        writer.writeheader()
        writer.writerows(rows)  
    print("Preprocessed full data saved to ", intermediate_file_path)

    # Read the intermediate data file as pandas dataframe for further processing
    df_initial = pd.read_csv(intermediate_file_path)
    print("Initial dataframe:")
    print(df_initial.head(20))
    print(df_initial.shape)
    print(df_initial.dtypes)

    # shuffle the dataframe
    df_initial = df_initial.sample(frac=1, random_state=42)
    print("Shuffled dataframe:")
    print(df_initial.head(20))
    print(df_initial.shape)
    print(df_initial.dtypes)
    shuffled_file_path = data_file_path.with_name(data_file_path.name.replace(".csv", "_preprocessed_full_data.csv"))
    df_initial.to_csv(shuffled_file_path, index=False)
    print("Full shuffled dataframe saved to ", shuffled_file_path)

    # df.iloc starts indexing from 0
    for i in range(0, len(df_initial), chunk_size):
        print("\n\nI: ", i)
        chunk = df_initial.iloc[i:i+chunk_size]
        chunk_number = i//chunk_size + 1
        print(f"Chunk {chunk_number}: ")
        print(f"Number of yes in the chunk: {chunk[chunk['gold_label'] == YES].shape[0]}")
        print(f"Number of no in the chunk: {chunk[chunk['gold_label'] == NO].shape[0]}")

        print("First 20 records of the chunk: \n")
        print(chunk.head(20).to_string(index=False))
        print("Shape of the chunk: \n", chunk.shape)
        print("Data types of the chunk: \n", chunk.dtypes)

        # remove the gold columns 'gt_label' and 'gold_label'
        chunk = chunk.drop(columns=['gt_label', 'gold_label'])
        print("Chunk after removing the gold columns: \n", chunk.head(20).to_string(index=False))
        print("Shape of the chunk after removing the gold columns: \n", chunk.shape)
        print("Data types of the chunk after removing the gold columns: \n", chunk.dtypes)
        
        chunk_file_path = data_file_path.with_name(data_file_path.name.replace(".csv", f"_preprocessed_{chunk_number}.csv"))
        print(f"Chunk {chunk_number} saved to {chunk_file_path}")
        chunk.to_csv(chunk_file_path, index=False)
        


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
    
    chunk_size = 500
    print(f"--- Testing prepare_data_chunks with chunk size={chunk_size} ---")
    prepare_data_chunks(dummy_exp_dir, chunk_size)