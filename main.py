from functions import *
import logging
import time


def main():
    while True:
        file_name = file_checker()
        if file_name is None:
            time.sleep(10)
            logging.info('no new files, retry in 10 seconds')
            continue
        else:
            logging.info(f'new file {file_name} is found. processing...')
            df = read_file(file_name) # reads file 
            rm = result_minutes(df)
            rh = result_hours(df)
            write_tg(f'File {file_name} is parsed. Found {rm} errors less than in 10 minutes')
            time.sleep(5)
            write_tg(f'File {file_name} is parsed. Found {rh} errors less than in one hour for bundle_id')
            time.sleep(10)
            logging.info(f'file {file_name} successfully uploaded, report is sent')
            continue


"""reset db each time before we run the code"""


file_dict.clear()


"""run the code"""


main()
