
from module import *


EXCEL_FILE = r'log_data\temp.xlsx'
CSV_FILE = r'csv_data\Number.csv'
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
CREDS_FILE = "sheet.json"  
SHEET_NAME = 'Concat'  
OUTPUT_DIR = r'D:\Output\Number'
USED_LOG_FILE = r'log_data\number.log'
SHEET_INDEX = 0

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
        durations, last_used, file_paths, csv_df = prepare_original_data(CSV_FILE)
        if csv_df is None:
            print("Failed to load data from CSV. Exiting.")
            return
        used_video_paths = load_used_videos(USED_LOG_FILE)
        results = []
        newly_used_paths = set()
        for i in range(len(suitable_df)):
            num_lists = 1
            desired_length = float(suitable_df.iloc[i]['desired length']) * 60
            first_vid_number = str(suitable_df.iloc[i]['first vids'])

            first_vd = find_first_vid(first_vid_number)
            first_path, first_duration = first_vd[0], convert_time_to_seconds(first_vd[1])
            if not first_path:
                print(f"Không tìm thấy video đầu tiên cho {first_vid_number}")
                continue

            for list_index in range(num_lists):
                available_indexes = [
                    idx for idx in range(len(file_paths))
                    if file_paths[idx] not in used_video_paths
                ]
                # Reset nếu đã dùng hết
                if not available_indexes:
                    print("Đã dùng hết video, reset log.")
                    used_video_paths.clear()
                    available_indexes = list(range(len(file_paths)))
                total_duration = first_duration
                selected_paths = [first_path]
                newly_used_paths.add(first_path)

                while available_indexes and total_duration < desired_length:
                    chosen_index = random.choice(available_indexes)
                    path = file_paths[chosen_index]
                    if path not in used_video_paths:
                        total_duration += durations[chosen_index]
                        selected_paths.append(path)
                        newly_used_paths.add(path)
                    available_indexes.remove(chosen_index)
                results.append({
                    'name': first_vid_number,
                    'group_index': i,
                    'list_number': list_index + 1,
                    'selected_files': selected_paths,
                    'total_duration': total_duration
                })
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
        filename = f"{name}_Tuan_number_ghep.mp4"
        output_path = os.path.join(OUTPUT_DIR, filename)
        auto_concat(ls['selected_files'], output_path)

        group_index = ls['group_index']
        row_index = suitable_df.index[group_index]

        current_value = original_df.at[row_index, 'output directory']
        if pd.isna(current_value) or str(current_value).strip().lower() == 'nan' or current_value == "":
            original_df.at[row_index, 'output directory'] = output_path
        else:
            original_df.at[row_index, 'output directory'] = f"{current_value}\n{output_path}"

        original_df.at[row_index, 'status'] = 'Done'
        original_df.at[row_index, 'number_of_vids'] = 1

    #Lưu file Excel & cập nhật Google Sheet
    try:
        if 'number_of_vids' in original_df.columns:
            original_df = original_df.drop(columns=['number_of_vids'])
        original_df.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
        print("Saved all Excel content into Excel file")
        excel_to_sheet(EXCEL_FILE, SHEET_NAME,1)
        print("Updated Google Sheet.")
    except Exception as e:
        print(f"Error: {e}")

    #Lưu log video đã dùng
    used_video_paths.update(newly_used_paths)
    save_used_videos(USED_LOG_FILE, used_video_paths)

if __name__ == '__main__':
    main()
