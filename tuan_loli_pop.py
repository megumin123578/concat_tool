
from module import *


EXCEL_FILE = r'log_data\temp.xlsx'
CSV_FILE = r'csv_data\Lolipop.csv'
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDS_FILE = "sheet.json"  
SHEET_NAME = 'Concat'  
OUTPUT_DIR = r'D:\Output\Lollipop'
USED_LOG_FILE = r'log_data\Lollipop.log'
SHEET_INDEX = 3
NAME_FILE = 'Lollipop'

def main():
    try:
        creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)
        copy_from_ggsheet_to_excel(gc, SHEET_NAME, EXCEL_FILE, SHEET_INDEX)
    except Exception as e:
        print(f"Error in main execution: {e}")
        return
    try:
        suitable_df, original_df = pre_process_data(EXCEL_FILE)
        if suitable_df.empty:
            print("No suitable data found for processing (status='auto' with non-null 'first vids' and 'desired length').")
            return
        durations, file_paths, csv_df = prepare_original_data(CSV_FILE)
        if csv_df is None:
            print("Failed to load data from CSV. Exiting.")
            return
        used_video_paths = load_used_videos(USED_LOG_FILE)
        results, newly_used_paths = generate_video_lists(
            suitable_df=suitable_df,
            durations=durations,
            file_paths=file_paths,
            used_video_paths=used_video_paths,
            num_lists=1
        )
        if not results:
            print("No video lists generated.")
            return
        format_and_print_results(results)
    except FileNotFoundError:
        print(f"Error: File '{EXCEL_FILE}' not found.")
        return
    except KeyError as e:
        print(f"Error: Missing column {e} in the Excel file.")
        return
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return

    # Bước 3: Ghép video + cập nhật Excel
    for ls in results:
        name = get_file_name(ls['name'])
        filename = f"{name}_{NAME_FILE}.mp4"
        output_path = os.path.join(OUTPUT_DIR, filename)
        auto_concat(ls['selected_files'], output_path)
        mapping_log = r"log_data\mapping_log\lolipop.log"
        os.makedirs(os.path.dirname(mapping_log), exist_ok=True)
        with open(mapping_log, "a", encoding="utf-8") as f:
            f.write("\n==============================\n")
            f.write(f"OUTPUT: {output_path}\n")
            f.write("INPUTS:\n")
            for p in ls['selected_files']:
                f.write(f"{p}\n")
            f.write("\n==============================\n")

        group_index = ls['group_index']
        row_index = suitable_df.index[group_index]

        current_value = original_df.at[row_index, 'output directory']
        if pd.isna(current_value) or str(current_value).strip().lower() == 'nan' or current_value == "":
            original_df.at[row_index, 'output directory'] = output_path
        else:
            original_df.at[row_index, 'output directory'] = f"{current_value}\n{output_path}"

        original_df.at[row_index, 'status'] = 'Done'
        #Lưu file Excel & cập nhật Google Sheet
        
        original_df.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
        print(f"Saved updated Excel file to row {row_index}.")
        try:
            update_row_to_sheet(row_index, original_df.loc[row_index], SHEET_NAME, SHEET_INDEX)
            print(f"Updated Google Sheet to row {row_index}.")
        except Exception as e:
            print(f"Error updating Google Sheet: {e}")
    #Lưu log video đã dùng
    used_video_paths.update(newly_used_paths)
    save_used_videos(USED_LOG_FILE, used_video_paths)

if __name__ == '__main__':
    main()
