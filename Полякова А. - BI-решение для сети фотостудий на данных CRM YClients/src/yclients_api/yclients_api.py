from sqlalchemy import create_engine
from typing import Literal
import pandas as pd
import requests
import json
import time
import os


class YClientsApi:
    """
    YClientsApi
    ===========
    Для использования необходимо передать
    :param partner_token: токен аккаунта разработчика (bearer token)
    """
    def __init__(self, partner_token: str):
        self.date_start = None
        self.date_end = None
        self.date_changed_after = None
        self.date_changed_before = None
        self.company_id = None
        # Тут хранится файл attributues из него мы можем взять как сущности
        # так и нужные столбцы для сущностей
        self.attributes = json.load(open('src/yclients_api/attributes.json'))
        self.headers = {
            'Accept': 'application/vnd.yclients.v2+json',
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {partner_token}'
        }
        self._show_all_attributes = False
        self._show_debugging = False

    """ATTRIBUTES"""

    def is_show_all_attributes(self, _show_all_attributes: bool):
        """
        :param _show_all_attributes: bool, отвечает за то, выводить все ли атрибуты сущности
        """
        self._show_all_attributes = _show_all_attributes

    """DEBUGGING"""

    def is_show_debugging(self, _is_show_debugging: bool):
        print("Debugging prints turned on")
        self._show_debugging = _is_show_debugging

    def set_company_id(self, company_id):
        self.company_id = company_id

    """DATES"""

    def set_dates(self, date_start: str = None, date_end: str = None):
        """
        :param date_start: начало выгрузки данных, формат ISO
        :param date_end: начало выгрузки данных, формат ISO
        """
        if date_start and date_end is not None:
            self.date_start = date_start
            self.date_end = date_end
            self.date_changed_after = f'{date_start}T00:00:00'
            self.date_changed_before = f'{date_end}T23:59:59'

    """REQUESTS"""

    def _make_request(self, 
                      method: Literal['GET', 'POST'], 
                      url: str, 
                      payload: dict = None) -> dict:
        try:
            time.sleep(0.5)
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=payload, timeout=40)
                return response.json()
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=payload, timeout=40)
                return response.json()
            else:
                raise ValueError("Недопустимый метод запроса")
        except requests.exceptions.RequestException as e:
            print(f'Ошибка при запросе данных по URL {e}')
            return {}
    
    def _normalize_meta_data(self, 
                             data: pd.DataFrame, 
                             col: str, 
                             entity_name: str = None) -> pd.DataFrame:
        """
        :param data: данные из которых нужно вытянуть метаданные
        :param col: название столбца в котором содержатся метаданые
        :param entity_name: название столбца по которому формируется ключ
        """
        md = []
        for idx, row in data.iterrows():
            if len(row[col]) > 0:
                temp = pd.json_normalize(row[col])
                if col == 'services':
                    temp[entity_name] = row[entity_name]
                    temp['date'] = row['date']
                    if any(d > 0 for d in temp['discount']):
                        data.at[idx, 'discount_services'] = True 
                temp['company_id'] = self.company_id
                temp['visit_attendance'] = row['visit_attendance']
                md.append(temp)
        if md:
            data_meta = pd.concat(md, ignore_index=True)
            return data_meta
        return None

    """USER AUTHORIZATION"""

    def update_authorization(self, 
                             *,
                             user_token: str = None,
                             login: str = None, 
                             password: str = None):
        """
        Для получения некоторых запросов необходимо получить токен пользователя.

        Parameters
        ----------
        :param user_token: авторизационный токен пользователя
        :param login: yclients логин пользователя
        :param password: yclients пароль пользователя
        """
        if user_token is None:
            url = 'https://api.yclients.com/api/v1/auth'
            payload = {
                "login": login,
                "password": password
            }
            response = self._make_request('POST', url, payload)
            user_token = response['user_token']

        self.headers['Authorization'] = self.headers['Authorization'] + f', User {user_token}'
    
    """COMPANIES"""
    
    def get_companies(self, active: int = 0) -> pd.DataFrame:
        """
        Возвращает информацию о филиалах
        :param active: 1 для активных филиалов, 0 для всех
        """
        url = f'https://api.yclients.com/api/v1/companies'
        payload = {
            'my': 1,
            'active': active
        }
        response = self._make_request('GET', url, payload)
        data = pd.json_normalize(response['data'])
        
        if data.empty:
            print('Нет данных о филиалах!')
            return None
        
        data.columns = [col.replace('.', '_') for col in data.columns]
        data = data.rename({'id': 'company_id'}, axis=1)

        if not self._show_all_attributes:
            data = data[self.attributes['companies']]
        return data
    
    """SERVICES"""

    def get_company_services(self) -> pd.DataFrame:
        """
        Возвращает информацию о услугах филиала
        """
        url = f'https://api.yclients.com/api/v1/company/{self.company_id}/services/'
        response = self._make_request('GET', url)
        data = pd.json_normalize(response['data'])

        if data.empty:
            return None
        
        url = f'https://api.yclients.com/api/v1/chain/{197381}/service_categories'
        response = self._make_request('GET', url)
        data_categories = pd.json_normalize(response['data'])

        data = pd.merge(data, data_categories, left_on='category_id', right_on='id')

        data = data.rename({'id_x': 'service_id', 'title_x': 'service_title',
                             'title_y': 'category_title'}, axis=1)    
        data['company_id'] = self.company_id

        if not self._show_all_attributes:
            data = data[self.attributes['services']]
        return data
    
    """STAFF"""

    def get_company_staff(self) -> pd.DataFrame:
        """
        Возвращает информацию о сотрудниках филиалах
        """
        url = f'https://api.yclients.com/api/v1/company/{self.company_id}/staff/'
        response = self._make_request('GET', url)
        data = pd.json_normalize(response['data'])

        if data.empty:
            return None
        
        data['rating'] = data['rating'].astype('float')
        data = data.rename({'id': 'staff_id'}, axis=1)

        if self._show_all_attributes is False:
            data = data[self.attributes['staff']]
        return data   
    
    def _get_employee_schedule(self, staff_id) -> pd.DataFrame:
        """
        :param staff_id: ID сотрудника по которому нужно выгрузить расписание
        """
        url = f'https://api.yclients.com/api/v1/schedule/{self.company_id}/{staff_id}/{self.date_start}/{self.date_end}'
        response = self._make_request('GET', url)
        parsed_data = []
        for entry in response['data']:
            date = entry['date']
            slots = entry['slots']
            if slots:
                hours1, minutes1 = map(int, slots[0]['from'].split(':'))
                time_decimal1 = hours1 + minutes1 / 60
                hours2, minutes2 = map(int, slots[-1]['to'].split(':'))
                time_decimal2 = hours2 + minutes2 / 60
                parsed_data.append({
                    'company_id': self.company_id,
                    'staff_id': staff_id,
                    'date': date,
                    'is_working': True,
                    'from': slots[0]['from'],
                    'to': slots[-1]['to'],
                    'decimal_time': time_decimal2 - time_decimal1
                })
            else:
                parsed_data.append({
                    'company_id': self.company_id,
                    'staff_id': staff_id,
                    'date': date,
                    'is_working': False,
                    'from': None,
                    'to': None,
                    'decimal_time': None
                })
        data = pd.json_normalize(parsed_data)
        return data
    
    def get_company_staff_schedule(self) -> pd.DataFrame:
        """
        Возвращает информацию о филиалах
        """
        staff = self.get_company_staff()
        schedule = pd.DataFrame(columns=self.attributes['staff_schedule'])
        for _, emp in staff.iterrows():
            if self._show_debugging:
                emp_name = emp['name']
                print(f'-- Выгрузка по сотруднику {emp_name} филиала ID {self.company_id}')
            data = self._get_employee_schedule(emp['staff_id'])
            schedule = pd.concat([schedule, data], ignore_index=True)
        return schedule.sort_values(by=['date']).reset_index(drop=True)

    """CLIENTS"""

    def get_company_clients(self) -> pd.DataFrame:
        url = f'https://api.yclients.com/api/v1/company/{self.company_id}/clients/search'
        payload = {
            'page': 1,
            'page_size': 200,
            'fields': ['id', 'name', 'phone', 'email', 'discount', 
                       'first_visit_date', 'last_visit_date', 'sold_amount', 'visits_count'],
            'order_by': 'id',
            'order_by_direction': 'ASC',
            'operation': 'AND',
            'filters': [{
                'type': 'record',
                'state': {
                    'created': {
                        'from': self.date_start,
                        'to': self.date_end
                    }
                }
            }]
        }
        response = self._make_request('POST', url, payload)

        if response['meta']['total_count'] == 0:
            print(f'-- Warning! Компания под ID {self.company_id} не имеет данных о клиентах за данных промежуток')
            return None
        print('-- В базе найдено {0} клиентов'.format(response['meta']['total_count']))
        data_list = []
        while True:
            response = self._make_request('POST', url, payload)
            if not response['data']:
                break
            data_list.extend(response['data'])
            if self._show_debugging:
                print('-- Страница {0} загружена'.format(payload["page"]))
            payload['page'] += 1

        data = pd.json_normalize(data_list)
        
        data = data.rename({'id': 'client_id'}, axis=1)
        data['company_id'] = self.company_id

        if not self._show_all_attributes:
            data = data[self.attributes['clients']]
        return data
    
    """RECORDS"""

    def get_company_records(self, 
                            key: Literal['records', 'records_services', 'records_transactions']
                            ) -> pd.DataFrame:
        url = f'https://api.yclients.com/api/v1/records/{self.company_id}'
        payload = {
            'count': 200,
            'page': 1,
            'with_deleted': 1,
            'include_finance_transactions': 1,
            'start_date': self.date_start,
            'end_date': self.date_end
        }
        response = self._make_request('GET', url, payload)
        if response['meta']['total_count'] == 0:
            print(f'-- Warning! Компания под ID {self.company_id} не имеет данных о записях за данных промежуток')
            return None
        print('-- В базе найдено {0} записей'.format(response['meta']['total_count']))
        data_list = []
        while True:
            response = self._make_request('GET', url, payload)
            if not response['data']:
                break
            data_list.extend(response['data'])
            if self._show_debugging:
                print('-- Страница {0} загружена'.format(payload['page']))
            payload['page'] += 1

        records = pd.json_normalize(data_list)
        records = records.rename({'id': 'record_id'}, axis=1)
        records = records.drop(['staff.id'], axis=1)
        records.columns = [col.replace('.', '_') for col in records.columns]
        records['record_from'] = records['record_from'].replace('', 'Администратор')
        records['discount_services'] = False

        try:
            records['client_id'] = records['client_id'].fillna(-1)
            records['client_id'] = records['client_id'].astype('int')
            records['client_success_visits_count'] = records['client_success_visits_count'].fillna(-1)
            records['client_success_visits_count'] = records['client_success_visits_count'].astype('int')
            records['client_fail_visits_count'] = records['client_fail_visits_count'].fillna(-1)
            records['client_fail_visits_count'] = records['client_fail_visits_count'].astype('int')
        except:
            records['client_id'] = -1
            records['client_fail_visits_count'] = -1
            records['client_success_visits_count'] = -1

        records_transactions = self._normalize_meta_data(records, 'finance_transactions')
        if records_transactions is not None:
            records_transactions = records_transactions.rename({'id': 'transaction_id'}, axis=1)
            records_transactions = records_transactions.drop(['account.id'], axis=1)
            if 'client' in records_transactions.columns:
                records_transactions = records_transactions.drop(['client'], axis=1)
            records_transactions.columns = [col.replace('.', '_') for col in records_transactions.columns]

        records_services = self._normalize_meta_data(records, 'services', 'record_id')
        if records_services is not None:
            records_services = records_services.rename({'id': 'service_id'}, axis=1)
            records_services['discount'] = records_services['discount'].astype('float')

        loyalty_transactions = self.get_chain_loyalty_transactions()

        incorrect_records = set(records_services[(records_services['visit_attendance'] == 1) & (records_services['discount'] != 100)]['record_id']) \
                                           - set(records_transactions['record_id']) - set(loyalty_transactions['item_record_id'])
        
        records['is_correct_record'] = records['record_id'].apply(lambda row: True if row not in incorrect_records else False)
        records['payment_certificate'] = records['record_id'].apply(lambda row: True if row in loyalty_transactions['item_record_id'].values else False)

        def _get_status(row, rt, lt):
            if row['discount'] != 0:
                return 0
            elif row['record_id'] in rt['record_id'].values:
                return 1  
            elif row['record_id'] in lt['item_record_id'].values:
                return 2  
            else:
                return -1

        records_services['payment_status'] = records_services.apply(lambda row: _get_status(row, records_transactions, loyalty_transactions), axis=1)

        if key == 'records_services':
            if self._show_debugging:
                print(f'-- В базе найдено {records_services.shape[0]} записей услуг') 
            if not self._show_all_attributes:
                records_services = records_services[self.attributes['records_services']]
            return records_services 

        elif key == 'records_transactions':
            if self._show_debugging:
                print(f'-- В базе найдено {records_transactions.shape[0]} записей транзакций')
            if not self._show_all_attributes:
                records_transactions = records_transactions[self.attributes['records_transactions']]
            records_transactions = records_transactions[['transaction_id', 'document_id', 'date', 'type_id', 'expense_id',
                'account_id', 'amount', 'client_id', 'master_id', 'supplier_id',
                'comment', 'item_id', 'target_type_id', 'record_id',
                'goods_transaction_id', 'master', 'supplier', 'expense_id',
                'expense_title', 'expense_type', 'account_title', 'account_is_cash',
                'account_is_default', 'client_id', 'client_name', 'client_phone',
                'client_email', 'company_id', 'visit_attendance']]
            return records_transactions
            
        elif key == 'records':
            if not self._show_all_attributes:
                records = records[self.attributes['records']]
            return records
        

    """TR"""

    def get_company_transactions(self):
        url = f'https://api.yclients.com/api/v1/transactions/{self.company_id}'
        payload = {
            'count': 200,
            'page': 1,
            'deleted': 1,
            'start_date': self.date_start,
            'end_date': self.date_end
        }
        response = self._make_request('GET', url, payload)
        if not response['data']:
            print(f'-- Warning! Компания под ID {self.company_id} не имеет данных о транзакциях за данных промежуток')
            return None
        data_list = []
        while True:
            response = self._make_request('GET', url, payload)
            if not response['data']:
                break
            data_list.extend(response['data'])
            if self._show_debugging:
                print('-- Страница {0} загружена'.format(payload["page"]))
            payload['page'] += 1

        data = pd.json_normalize(data_list)
        return data
    
    """GOODS"""

    def get_company_goods(self) -> pd.DataFrame:
        url = f'https://api.yclients.com/api/v1/goods/{self.company_id}'
        response = self._make_request('GET', url)
        data = pd.json_normalize(response['data'])

        if data.empty:
            print(f'-- Warning! Компания под ID {self.company_id} не имеет данных о товарах')
            return None
        
        data = data.rename({'salon_id': 'company_id'}, axis=1)

        if not self._show_all_attributes:
            data = data[self.attributes['goods']]
        return data 
    
    def get_company_goods_transactions(self, count: int = 200) -> pd.DataFrame:
        '''
        Yclients не возвращает общее количество транзакцией при запросе,
        но если указать очень больше количество отображаемых транзакций, 
        то он выведет все доступные транзакции. То есть если указать 50000
        при имеющихся 20000 о которых неизвестно, то он вернет только 20000.
        :param count: количество транзакций на странице
        :param start_date: фильтр даты, с этой даты начинается выгрузка
        :param end_date: фильтр даты, с этой даты заканчивается выгрузка
        '''
        url = f'https://api.yclients.com/api/v1/storages/transactions/{self.company_id}'
        querystring = {
            'count': count,
            'start_date': self.date_start,
            'end_date': self.date_end,
        }
        response = self._make_request('GET', url, querystring)
        data = pd.json_normalize(response['data'])

        if data.empty:
            print(f'-- Warning! Компания под ID {self.company_id} не имеет данных о транзакциях товаров за данных промежуток')
            return None
        
        data = data.rename({'id': 'transaction_id'}, axis=1)
        data['company_id'] = self.company_id
        data.columns = [col.replace('.', '_') for col in data.columns]

        if not self._show_all_attributes:
            data = data[self.attributes['goods_transactions']]
        return data     

    """CATEGORIES"""

    def get_services_categories_chain(self, chain_id: int | str = 197381) -> pd.DataFrame:
        """
        :param chain_id: идентификатор сети компаний
        """
        url = f'https://api.yclients.com/api/v1/chain/{chain_id}/service_categories'
        response = self._make_get_request(url)
        data = pd.json_normalize(response['data'])

        return data

    def get_company_categories_entity(self, entity: Literal[1, 2] = 1) -> pd.DataFrame:
        """
        :param entity: объект категории: 1 - категории клиентов, 2 - категории записей
        """
        url = f'https://api.yclients.com/api/v1/labels/{self.company_id}/{entity}'
        querystring = {
            'company_id': self.company_id,
            'entity': entity
        }
        response = self._make_request('GET', url, querystring)
        data = pd.json_normalize(response['data'])

        if entity == 1:
            data = data.rename({'id': 'client_category_id',
                               'salon_id': 'company_id'}, axis=1)
        elif entity == 2:
            data = data.rename({'id': 'record_category_id',
                                'salon_id': 'company_id'}, axis=1)
            
        return data
    
    def show_user_permissions(self):
        """
        :return: выводит json-объект с правами доступа пользователя
        """
        url = f"https://api.yclients.com/api/v1/user/permissions/{self.company_id}"
        data = self._make_request('GET', url)
        print("User permissions:")
        print(json.dumps(data, indent=4, sort_keys=True))

    def get_chain_loyalty_transactions(self, chain_id=197381):
        # Получить список транзакций лояльности в сети
        url = f'https://api.yclients.com/api/v1/chain/{chain_id}/loyalty/transactions'

        querystring = {
            'created_after': self.date_start,
            'created_before': self.date_end,
            'count': 200
        }
        response = self._make_request('GET', url, querystring)
        data = pd.json_normalize(response['data'])

        if data.empty:
            print(f'-- Warning! Компания под ID {self.company_id} не имеет данных о списании с сертификатов за данных промежуток')
            return None

        data = data.drop(['type_id'], axis=1)
        data.columns = [col.replace('.', '_') for col in data.columns]

        return data

    """ALL CHAIN DATA"""

    def get_chain_data_all(self, 
                           *, 
                           path: str = None, 
                           properties: dict = None):
        """
        :param folder_name: название папки для выгрузки excel таблиц
        :param properties: параметры подключения к БД PostgreSQL
        """
        print(f'Выгрузка за период {self.date_start} - {self.date_end}')
        # Вызываем 2 функции тут, так как они возвращают информацию сразу по всем филиалам
        companies = self.get_companies(active=1)
        loyalty_transactions = self.get_chain_loyalty_transactions(197381)

        # Перечисляем по каким функциям нужно сделать цикл
        # Если self.get_company_staff то это функция, 
        # а если self.get_company_staff() это уже результат функции
        # Скобочки () отвечают за вызов функции
        need_to_get = [
            self.get_company_staff,
            self.get_company_staff_schedule,
            self.get_company_services,
            self.get_company_clients,
            self.get_company_records,
            self.get_company_records,
            self.get_company_records,
            self.get_company_goods,
            self.get_company_goods_transactions
        ]

        merged_data = {}
        for entity in list(self.attributes.keys())[2:]:
            merged_data[entity] = pd.DataFrame(columns=self.attributes[entity])

        for _, com in companies.iterrows():
            company_name = com['title']
            self.company_id = com['company_id']
            print(f'Выгрузка по филиалу - {company_name} ID {self.company_id}')
            for key, method in zip(list(self.attributes.keys())[2:], need_to_get):
                print(f'- Выгрузка таблицы {key}')
                if key in ['records', 'records_services', 'records_transactions']:
                    data = method(key)
                else:
                    data = method()
                if data is not None:
                    if not merged_data[key].empty:
                        merged_data[key] = pd.concat([merged_data[key], data], ignore_index=True)
                    else:
                        merged_data[key] = data

        if path is not None:
            if not os.path.exists(path):
                os.makedirs(path)
            for key in list(self.attributes.keys())[2:]:       
                file_path = os.path.join(path, key + '.xlsx')
                merged_data[key].to_excel(file_path, index=False, sheet_name=f'{key}')
                print(f'Таблица {key} загружена в {file_path}!')
 
            file_path = os.path.join(path, 'companies.xlsx')
            companies.to_excel(file_path, index=False, sheet_name='companies')
            print(f'Таблица companies загружена в {file_path}!')

            if loyalty_transactions is not None:
                file_path = os.path.join(path, 'loyalty_transactions.xlsx')
                loyalty_transactions.to_excel(file_path, index=False, sheet_name='loyalty_transactions')
                print(f'Таблица loalty_transactins загружена в {file_path}!')

        if properties is not None:
            host = properties['host']
            port = properties['port']
            database = properties['db_name']
            username = properties['username']
            password = properties['password']
            # Формируем что-то наподобие коннектора к БД
            engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}',
                                   connect_args={'options': '-c client_encoding=UTF8'})

            # Список таблиц справочников (в clients тоже нужно добавлять клиентов в конец, наверное???)
            insert_keys = ['companies', 'staff', 'services', 'goods']
            for key in list(self.attributes.keys())[2:]:
                # Если название сущности в таблицах справочниках, то меняем метод вставки
                if key in insert_keys:
                    if_exists = 'replace'
                else:
                    if_exists = 'append'
                merged_data[key].to_sql(name=f'{key}', con=engine, if_exists=if_exists, index=False)
                print(f'Таблица {key} загружена в postgresql!')

            companies.to_sql(name='companies', con=engine, if_exists='replace', index=False)
            print(f'Таблица companies загружена в postgresql!')

            if loyalty_transactions is not None:
                loyalty_transactions.to_sql(name='loyalty_transactions', con=engine, if_exists='append', index=False)
                print(f'Таблица loyalty_transactions загружена в postgresql!')
