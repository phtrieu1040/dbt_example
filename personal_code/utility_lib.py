import os
import re
import csv
import pytz
from datetime import datetime, timedelta
import pandas as pd
from typing import Literal

class MyFunction:
    @classmethod
    def _to_list(cls, value):
        result = []
        value_type = type(value)
        if value_type is list:
            return value
        else:
            if value_type is int or value_type is str:
                result.append(value)
                return result
            else:
                return None

    @classmethod
    def full_outer_join_2_list(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list = lista
        idxa = 0
        idxb = 0
        lena = len(lista)
        lenb = len(listb)
        while idxa < lena and idxb < lenb:
            if lista[idxa] == listb[idxb]:
                idxb += 1
                idxa = 0
            else:
                if idxa == lena - 1:
                    joined_list.append(listb[idxb])
                    idxb += 1
                    idxa = 0
                else:
                    idxa += 1
        return joined_list
    
    @classmethod
    def inner_join_2_list(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list = []
        idxa = 0
        idxb = 0
        lena = len(lista)
        lenb = len(listb)
        while idxa < lena and idxb < lenb:
            if lista[idxa] == listb[idxb]:
                joined_list.append(listb[idxb])
                idxb += 1
                idxa = 0
            else:
                if idxa == lena - 1:
                    idxb += 1
                    idxa = 0
                else:
                    idxa += 1
        return joined_list
    
    @classmethod
    def non_outer_join_2_list(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list1 = cls.non_outer_join_a_vs_b(a = listb, b = lista)
        joined_list2 = cls.non_outer_join_a_vs_b(a = lista, b = listb)
        combined_list = joined_list1 + joined_list2
        return combined_list

    @classmethod
    def non_outer_join_a_vs_b(cls, a, b):
        lista = cls._to_list(a)
        listb = cls._to_list(b)
        joined_list = []
        idxa = 0
        idxb = 0
        lena = len(lista)
        lenb = len(listb)
        while idxa < lena and idxb < lenb:
            if lista[idxa] == listb[idxb]:
                idxb += 1
                idxa = 0
            else:
                if idxa == lena - 1:
                    joined_list.append(listb[idxb])
                    idxb += 1
                    idxa = 0
                else:
                    idxa += 1
        return joined_list
    
    @classmethod
    def generate_date_list(cls, start_date, end_date=None, ascending=True):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        try:
            if end_date:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            else:
                end_date = datetime.today()
        except Exception as e:
            print('Incorrect date format error: ',e)
            return False

        if ascending == True:
            date_list=MyFunction._generate_date_list_asc(start_date=start_date, end_date=end_date)
        elif ascending == False:
            date_list=MyFunction._generate_date_list_desc(start_date=start_date, end_date=end_date)
        else:
            print('Incorrect Order')
            return False
        return date_list

    @classmethod
    def _generate_date_list_desc(cls, start_date, end_date):
        date_list = []
        current = end_date
        while current >= start_date:
            date_list.append(current.strftime("%Y-%m-%d"))
            current -= timedelta(days=1)
        return date_list

    @classmethod
    def _generate_date_list_asc(cls, start_date, end_date):
        date_list = []
        current = start_date
        while current <= end_date:
            date_list.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return date_list

    @classmethod
    def extract_sheet_id(cls, sheet_url):
        start_index = sheet_url.find("/d/")
        if start_index != -1:
            start_index += 3  # Move to the character after "/d/"
            end_index = sheet_url.find("/", start_index)  # Find the next "/"
            if end_index == -1:
                sheet_id = sheet_url[start_index:]
            else:
                sheet_id = sheet_url[start_index:end_index]
            return sheet_id
        else:
            return None

    @staticmethod
    def extract_google_drive_id(url):
        # Define the patterns for Google Drive documents
        google_drive_patterns = [
            # Common document formats
            "/spreadsheets/d/",
            "/document/d/",
            "/presentation/d/",
            "/file/d/",
            "/forms/d/",
            "/drawings/d/",
            
            # Folders and drives
            "/drive/folders/",
            "/drive/u/[0-9]+/folders/",
            "/drive/shared-drives/",
            "/drive/u/[0-9]+/shared-drives/",
        ]

        # Try to match standard patterns
        for pattern in google_drive_patterns:
            # Use regex to handle patterns with wildcards like u/[0-9]+
            if '[0-9]+' in pattern:
                pattern_regex = pattern.replace('[0-9]+', '[0-9]+')
                match = re.search(f"{pattern_regex}([^/?]+)", url)
                if match:
                    return match.group(1)
            else:
                start_index = url.find(pattern)
                if start_index != -1:
                    start_index += len(pattern)  # Move index to start of the ID
                    end_index = url.find("/", start_index)  # Find the next "/"
                    if end_index == -1:  # If no "/" found
                        end_index = url.find("?", start_index)  # Try finding a "?"
                        if end_index == -1:  # If no "?" found either
                            return url[start_index:]  # Take the rest as ID
                        else:
                            return url[start_index:end_index]  # Take until "?"
                    else:
                        return url[start_index:end_index]  # Take until "/"
        
        # Handle Google Apps Script URLs
        script_match = re.search(r'script\.google\.com/[a-z/]+/([^/?]+)', url)
        if script_match:
            return script_match.group(1)

        return None
    
    @classmethod
    def _read_sql(cls, sql_file_path):
        try:
            with open(sql_file_path, "r") as f:
                query_string = f.read()
        except Exception as e:
            print(f"Couldn't Read SQL File At {sql_file_path}: ", e)
            return False
        return query_string
    
    @classmethod
    def _read_csv(cls, file_path):
        try:
            with open(file_path, mode = 'r', newline='') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
                if rows:
                    return rows
                    # last_row = rows[-1]
                    # return int(last_row['row_number'])
        except Exception as e:
            print(e)
            return None

    @classmethod
    def _write_csv(cls, file_path, mode, data_to_write):
        with open(file_path, mode=mode, newline='', encoding="utf-8", errors="ignore") as file:
            writer = csv.writer(file)
            writer.writerow(cls._to_list(data_to_write))
    
    @classmethod
    def _write_csv_log(cls, file_path, key_column, *args):
        _data = list(args)
        _data.insert(1, cls._get_current_time('string'))
        # _data = args + (cls._get_current_time('string'),) #add write time
        data = _data
        if os.path.exists(file_path):
            all_rows = cls._read_csv(file_path=file_path)
            if all_rows is None:
                print('error getting last row number')
                return
            last_row = all_rows[-1]
            last_row_num = int(last_row['row_number'])
            row_number = last_row_num + 1
        else:
            column_names = []
            try:
                data_len = len(data)
            except Exception as e:
                print(e)
                return
            if data_len == 2:
                pass
            else:
                key = data[:2]
                non_key = data[2:]
                non_key_columns = cls.non_outer_join_a_vs_b(key, non_key)
                for column in non_key_columns:
                    column_name = input(f'Enter column name for column ({column}): ')
                    column_names.append(column_name)
            column_names_to_write = cls._to_list('row_number') + cls._to_list(key_column) + cls._to_list('write_time') + column_names
            mode = 'w'
            row_number = 1
            cls._write_csv(file_path=file_path, mode=mode, data_to_write=column_names_to_write)
        data_to_write = cls._to_list(row_number) + cls._to_list(data)
        try:
            cls._write_csv(file_path=file_path, mode='a', data_to_write=data_to_write)
        except Exception as e:
            print(e)

    @classmethod
    def sheet_id_to_url(cls,sheets_id):
        base_url = "https://docs.google.com/spreadsheets/d/"
        return f"{base_url}{sheets_id}/edit"
    
    @classmethod
    def check_dup_df_column_name(cls,df):
        column_list = df.columns.tolist()
        dup_columns = pd.DataFrame({'column':column_list}).groupby('column').agg(count=('column','count')).reset_index().query('count>1').column.tolist()
        if len(dup_columns) == 0:
            return
        for column in dup_columns:
            print(column)
    
    @classmethod
    def procedure_and_logging(cls, log_path, input_list, main_func, key_column='key_column', *args):
        input_list_number = ''
        while True:
            input_limit = input('Perform for all input? (y/n): ')
            if input_limit == 'y':
                break
            elif input_limit == 'n':
                while True:
                    limit = input('Number of input to run: ')
                    try:
                        input_list_number = int(limit)
                    except:
                        print('Please enter a number')
                        continue
                    break
                break
            else:
                continue
        
        if input_list_number == '':
            pass
        else:
            input_list = input_list[:input_list_number]
        
        _input_list = [str(i) if isinstance(i, bytes) else i for i in input_list]
        input_type = type(input_list[0])

        if os.path.exists(log_path):
            all_logs = cls._read_csv(file_path=log_path)
            if all_logs is None:
                return
            df_all_logs = pd.DataFrame(all_logs)
            completed_procedure = df_all_logs[key_column].to_list()
            _remaining_procedure = MyFunction.non_outer_join_a_vs_b(a=completed_procedure, b=_input_list)
            if input_type is bytes:
                remaining_procedure = [i[2:-1].encode('ascii') for i in _remaining_procedure]
            else: remaining_procedure = _remaining_procedure
        else:
            remaining_procedure = input_list
        for procedure in remaining_procedure:
            data = main_func(procedure, *args)
            if data is None:
                continue
            if isinstance(data, tuple):
                try:
                    cls._write_csv_log(log_path, key_column, *data)
                except Exception as e:
                    print('Error writing 01:', e)
            else:
                try:
                    cls._write_csv_log(log_path, key_column, data)
                except Exception as e:
                    print('Error writing 02:', e)

    @classmethod
    def _get_current_time(cls, type: Literal['date', 'time', 'datetime']='datetime'):
        timezone = pytz.timezone('Asia/Bangkok')
        current_time = datetime.now(timezone)
        checker = True
        while checker == True:
            if type == 'date':
                current_date_time = current_time.strftime("%Y-%m-%d")
                break
            elif type == 'time':
                current_date_time = current_time.strftime("T%H:%M:%S")
                break
            elif type == 'datetime':
                current_date_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
                break
            else:
                checker = False
                current_date_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
        return current_date_time


    @classmethod
    def _remove_accents(cls, input_str):
        s1 = u'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝàáâãèéêìíòóôõùúýĂăĐđĨĩŨũƠơƯưẠạẢảẤấẦầẨẩẪẫẬậẮắẰằẲẳẴẵẶặẸẹẺẻẼẽẾếỀềỂểỄễỆệỈỉỊịỌọỎỏỐốỒồỔổỖỗỘộỚớỜờỞởỠỡỢợỤụỦủỨứỪừỬửỮữỰựỲỳỴỵỶỷỸỹ'
        s0 = u'AAAAEEEIIOOOOUUYaaaaeeeiioooouuyAaDdIiUuOoUuAaAaAaAaAaAaAaAaAaAaAaAaEeEeEeEeEeEeEeEeIiIiOoOoOoOoOoOoOoOoOoOoOoOoUuUuUuUuUuUuUuYyYyYyYy'
        s = ''
        for c in input_str:
            if c in s1:
                s += s0[s1.index(c)]
            else:
                s += c
        return s
    
    @classmethod
    def _remove_strange_symbols(cls, input_str):
        import re
        pattern = re.compile(r'[^a-zA-Z0-9\s]')
        filtered_data = [pattern.sub('', item) for item in input_str]
        return filtered_data
    
    @classmethod
    def _remove_other_symbols(cls, input_str):
        # Fix replacements dictionary to use valid key/value pairs and valid characters
        replacements = {'\n': ' ', '\r': '', '"': '', '\u20ab': ''} 
        body = ''.join(replacements.get(c, c) for c in input_str)
        return body
        
    @classmethod
    def _transform_column_name_first_letter(cls, column_name):
        if column_name and column_name[0].isdigit():
            return 'n' + column_name
        else:
            return column_name
    
    @classmethod
    def _make_column_name_unique(cls, df):
        seen_names = set()
        new_columns = []
        for column in df.columns:
            new_column = column
            i = 2
            while new_column in seen_names:
                new_column = f"{column}_{i}"
                i += 1
            seen_names.add(new_column)
            new_columns.append(new_column)

        df.columns = new_columns

    @classmethod
    def standadize_column_name(cls,df):
        df.columns = (
            df.rename(columns={'':'blank'})
            .columns
            .str.lower()
            .str.replace(' ', '_')
            .str.replace('-', '')
            .str.replace('(', '')
            .str.replace(')', '')
            .str.replace('?', '')
            .str.replace('!', '')
            .str.replace('@', '')
            .str.replace('$', '')
            .str.replace('^', '')
            .str.replace('*', '')
            .str.replace(':', '')
            .str.replace('.', '_')
            .str.replace('&', 'and')
            .str.replace('%', 'percent')
            .str.strip()
        )
        df.columns = df.columns.map(cls._remove_accents)
        df.columns = df.columns.map(cls._transform_column_name_first_letter)
        cls._make_column_name_unique(df)

    @staticmethod
    def execute_function_and_log(func, success_message=None, failed_message=None, log_success_execution=False, path=None, log_name=None):
        import logging
        if path:
            if log_name:
                file_name = path+'/'+log_name
            else:
                file_name = path+'/app.log'
        else:
            if log_name:
                file_name = log_name
            else:
                file_name = 'app.log'

        logging.basicConfig(level=logging.DEBUG, filename=file_name, filemode='w',
                            format='%(asctime)s - %(levelname)s - %(message)s')
        try:
            func
            if log_success_execution:
                logging.info(f'Successfully {success_message}')
            else:
                pass
        except Exception as e:
            logging.error(f'Error {failed_message}', exc_info=True)

    
    @staticmethod
    def _get_input_num_type(message):
        while True:
            limit = input(f'{message}')
            try:
                input_number = int(limit)
            except:
                print('Please enter a number')
                continue
            break
        return input_number
    
    @staticmethod
    def _extract_email(text):
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(pattern, text)
        if match:
            return match.group()
        else:
            return None
        
    @staticmethod
    def _string_replace(string, **kwargs):
        result = ''.join(kwargs.get(c, c) for c in string)
        return result
    
    @staticmethod
    def convert_long_date_to_gmt7(date_string):
        date_string = date_string.split('(')[0].strip()
        time_adjust = int(date_string[-5:-2])
        
        date_format = "%a, %d %b %Y %H:%M:%S %z"
        dt = datetime.strptime(date_string, date_format)
        dt_gmt_plus_7 = dt + timedelta(hours=(7-time_adjust))
        result = dt_gmt_plus_7.strftime("%a, %d %b %Y %H:%M:%S")
        return result
    
    @staticmethod
    def list_to_single_string(input, delimiter=None, wrapper = None):
        if isinstance(input,(list,tuple,set)):
            pass
        else:
            print('input must be a list or tuple')
        try:
            single_string = delimiter.join(f"{wrapper}{item}{wrapper}" for item in input)
        except Exception as e:
            print('error: ', e)
            return
        return single_string
    
class Telegram:
    @classmethod
    def send_telegram_alert(cls, message, bot_token, chat_id):
        import requests
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": message}
            response = requests.post(url, data=payload, timeout=5)  # Prevent long hangs
            response.raise_for_status()
            print(f"✅ Sent message: {response.json()}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error: {e}")
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}")