from dotenv import load_dotenv
import os


columns = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model',
                  'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date',
                  'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr',
                  'ccpa', 'country_code', 'date']


load_dotenv()


TOKEN = os.getenv('TOKEN')
METHOD = os.getenv('METHOD')
CHAT_ID = os.getenv('CHAT_ID')

link = f'http://api.telegram.org/bot{TOKEN}/{METHOD}?chat_id={CHAT_ID}&text='
