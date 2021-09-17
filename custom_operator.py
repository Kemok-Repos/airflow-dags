from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook
from kemokrw.client_google import GoogleClient
from kemokrw.client_hubstaff import HubstaffClient
from kemokrw.client_teamwork import TeamworkClient
from kemokrw.client_zoho import ZohoClient
from kemokrw.extract_db import ExtractDB
from kemokrw.extract_file import ExtractFile
from kemokrw.extract_gsheet import ExtractGSheet
from kemokrw.extract_hubstaff import ExtractHubstaff
from kemokrw.extract_teamwork import ExtractTeamwork
from kemokrw.extract_zoho import ExtractZoho
from kemokrw.load_db import LoadDB
from kemokrw.load_file import LoadFile
# from kemokrw.load_gsheet impor LoadGSheet
from kemokrw.transfer_basic import BasicTransfer
# from kemokrw.transfer_db_date
# from kemokrw.transfer_db_key
from kontact.client import KontactClient
from typing import Optional
from jinja2 import Template
from utils import get_airflow_connection, read_file, get_params_from_query, join_dicts
from pprint import pprint
from pytz import timezone
from datetime import datetime

class DinamicTelegramOperator(BaseOperator):

    ui_color = '#EFBCD5'

    def __init__(
            self,
            *,
            telegram_conn_id: str = "kemok_telegram",
            chat_id: str = None,
            text: str = "Ningun mensaje a sido configurado.",
            params_conn_id: Optional[str] = None,
            params_query: Optional[str] = None,
            additional_params: Optional[dict] = None,
            **kwargs,
        ) -> None:
        self.chat_id = chat_id
        self.text = text 
        self.params_conn_id = params_conn_id
        self.additional_params = additional_params or dict()

        if telegram_conn_id is None:
            raise AirflowException("La connexion de Telegram no es valida.")

        self.telegram_conn_id = telegram_conn_id

        if params_conn_id and params_query:
            self.extra_params = True
            if params_query[-4:] == '.sql':
                try:
                    self.params_query = read_file(params_query, **kwargs)
                except Exception:
                    print('No fue posible leer el query del directorio {0}'.format(params_query))
                    self.extra_params = False
            elif params_query.strip() != '':
                self.params_query = params_query
            else:
                self.extra_params = False
        else:
            self.extra_params = False

        super().__init__(**kwargs)

    def execute(self, context):
        if self.extra_params:
            print('Extrayendo parametros adicionales desde la base de datos')
            print(self.params_query)
            query_params = get_params_from_query(self.params_conn_id, self.params_query)
            self.additional_params = join_dicts(query_params, self.additional_params)
        pprint(self.additional_params)
        self.text = Template(self.text, trim_blocks=True, lstrip_blocks=True).render(**self.additional_params)
        print('Chat ID: '+(self.chat_id or ''))
        print('Message:')
        print('\n'+self.text)

        telegram_hook = TelegramHook(
            telegram_conn_id=self.telegram_conn_id,
            token=None,
            chat_id=self.chat_id,
        )

        disable_notification = False
        time = datetime.now(timezone('America/Guatemala'))
        print(time)
        if time.hour < 7 or time.hour > 18:
            disable_notification = True

        if self.text:
            telegram_kwargs = dict()
            telegram_kwargs['text'] = self.text
            telegram_kwargs['disable_notification'] = disable_notification
            result = telegram_hook.send_message(telegram_kwargs)
        print(result)

class KontactOperator(BaseOperator):

    ui_color = '#EAD1CC'

    def __init__(
            self,
            *,
            kontact_key: str = None,
            campaign_id: str = None,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.kontact_key = kontact_key
        self.campaign_id = campaign_id

    def execute(self, context):
        client = KontactClient(self.kontact_key)
        result = client.send_campaign(int(self.campaign_id))
        pprint(result)

class TransferOperator(BaseOperator):

    ui_color = '#E4E6C3'

    def __init__(
            self,
            *,
            clase_transfer: str = None,
            clase_fuente: str = None,
            config_fuente: dict = None,
            query_fuente: str = None,
            clase_destino: str = None,
            config_destino: dict = None,
            query_destino: str = None,
            conn_id: str = None,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.clase_transfer = clase_transfer
        self.clase_fuente = clase_fuente
        self.config_fuente = config_fuente or dict()
        self.query_fuente = query_fuente or ''
        self.clase_destino = clase_destino
        self.config_destino = config_destino or dict()
        self.query_destino = query_destino or ''

    def execute(self, context):
        if self.query_fuente.strip() != '':
            param = get_params_from_query(self.conn_id, self.query_fuente)
            self.config_fuente = {**self.config_fuente, **param}
            if self.config_fuente == dict():
                raise AirflowException("Sin modelo fuente valido")

        if self.query_destino.strip() != '':
            param = get_params_from_query(self.conn_id, self.query_destino)
            self.config_destino = {**self.config_destino, **param}
            if self.config_destino == dict():
                raise AirflowException("Sin modelo destino valido")

        try:
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print('                         Creando fuente - {0}'.format(self.clase_fuente))
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print('')
            pprint(self.config_fuente)

            db_fuente = get_airflow_connection(self.config_fuente.get('connection'))

            if self.clase_fuente.lower() == 'extractdb':
                src = ExtractDB(db_fuente, self.config_fuente.get('table'), self.config_fuente.get('model'),
                                str(self.config_fuente.get('condition') or ''), str(self.config_fuente.get('order') or ''))
            elif self.clase_fuente.lower() == 'extractfile':
                src = ExtractFile(self.config_fuente.get('path'), self.config_fuente.get('model'), self.config_fuente.get('sheet'),
                                self.config_fuente.get('separator'), self.config_fuente.get('encoding'))
            elif self.clase_fuente.lower() == 'extractgsheet':
                src_client = GoogleClient(self.config_fuente.get('credential_file'), self.config_fuente.get('token_file'))
                src = ExtractGSheet(src_client, self.config_fuente.get('spreadsheetId'), self.config_fuente.get('range'),
                                    self.config_fuente.get('model'))
            elif self.clase_fuente.lower() == 'extracthubstaff':
                conn = BaseHook.get_connection(self.config_fuente.get('connection'))
                src_client = HubstaffClient(self.config_fuente.get('path'), str(conn.password),
                                            self.config_fuente.get('organization'))
                src = ExtractHubstaff(src_client, self.config_fuente.get('url'), self.config_fuente.get('endpoint'),
                                    self.config_fuente.get('endpoint_type'), self.config_fuente.get('response_key'),
                                    self.config_fuente.get('model'), self.config_fuente.get('params'), self.config_fuente.get('by_list'))
            elif self.clase_fuente.lower() == 'extractteamwork':
                conn = BaseHook.get_connection(self.config_fuente.get('connection'))
                src_client = TeamworkClient(str(conn.password))
                src = ExtractTeamwork(src_client, self.config_fuente.get('url'), self.config_fuente.get('endpoint'),
                                    self.config_fuente.get('endpoint_type'), self.config_fuente.get('response_key'),
                                    self.config_fuente.get('model'), self.config_fuente.get('params'), self.config_fuente.get('by_list'))
            elif self.clase_fuente.lower() == 'extractzoho':
                src_client = ZohoClient(self.config_fuente.get('path'))
                src = ExtractZoho(src_client, self.config_fuente.get('url'), self.config_fuente.get('endpoint'),
                                self.config_fuente.get('endpoint_type'), self.config_fuente.get('response_key'),
                                self.config_fuente.get('model'), self.config_fuente.get('params'), self.config_fuente.get('by_list'))
            elif self.clase_fuente.lower() == 'eltools':
                pass
            else:
                print(self.clase_fuente + ' no es una clase de fuente valida.')
                raise AirflowException("Error de clase. Revisar maestro_de_transferencias.")

            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print('                         Creando destino - {0}'.format(self.clase_destino))
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print('')
            pprint(self.config_destino)
            db_carga = get_airflow_connection(self.config_destino.get('connection'))

            if self.clase_destino.lower() == 'loaddb':
                dst = LoadDB(db_carga, self.config_destino.get('table'), self.config_destino.get('model'),
                            str(self.config_destino.get('condition') or ''), str(self.config_destino.get('order') or ''))
            elif self.clase_destino.lower() == 'loadfile':
                dst = LoadFile(self.config_destino.get('path'), self.config_destino.get('sheet'), self.config_destino.get('model'))
            elif self.clase_destino.lower() == 'eltools':
                pass
            else:
                print(self.clase_destino + ' no es una clase de destino valida.')
                raise AirflowException("Error de clase. Revisar maestro_de_transferencias.")
            
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print('                         Transfiriendo - {0}'.format(self.clase_transfer))
            print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
            print('')
            if self.clase_transfer.lower() == 'basictransfer':
                trf = BasicTransfer(src, dst)
                trf.transfer(2)
            elif self.clase_transfer.lower() == 'eltools':
                pass
            else:
                print(self.clase_transfer + ' no es una clase de transferencia valida.')
                raise AirflowException("Error de clase. Revisar maestro_de_transferencias.")

        except Exception as err:
            print(err)
            raise err