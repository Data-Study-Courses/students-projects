from src.yclients_api.yclients_api import YClientsApi
from src.yandex_metrics.yandex_metrics import get_data_yandex_metrics
from src.yclients_api.config import DATABASE_PROPERTIES, PATH_TO_SAVE_TO_EXCEL, PARTNER_TOKEN, USER_TOKEN
import json

# центр     216787
# взлетка   473965
# правый    679561
# свободный 924779
# абакан    937406

def main():

    START = '2024-02-01'
    END   = '2024-02-29'

    api = YClientsApi(PARTNER_TOKEN)
    api.update_authorization(user_token=USER_TOKEN)

    api.is_show_all_attributes(True)
    api.is_show_debugging(True)

    api.set_dates(START, END)
    api.set_company_id(473965)

    api.get_chain_data_all(path=PATH_TO_SAVE_TO_EXCEL)
    #get_data_yandex_metrics(start, end, path)


if __name__ == '__main__':
    main()