
## docs
# appstoreconnect - https://pypi.org/project/appstoreconnect/
# api - https://developer.apple.com/documentation/appstoreconnectapi

# importando libs
import pandas as pd
import stalse_functions as sf
import functions_framework

from datetime import datetime, timedelta
from appstoreconnect import Api, UserRole

    
# consumindo chaves do KMS para usar na chamada da API e do report
issuer_id = sf.keygcp('msc-issuerid-apple') # id único da Mosaic dentro da plataforma Apple
key_id = sf.keygcp('msc-keyid-apple') # id único do usuário dados@stalse
path_to_key_file = sf.keygcp('msc-privatekey-apple') # chave privada criada a partir do key_id
vendor_number = sf.keygcp('msc-vendor-apple') # id da mosaic para criar o relatório 'download_sales_and_trends_reports'


# criando client do appstoreconnect com as variáveis do KMS
api = Api(key_id, path_to_key_file, issuer_id)


# filtrando data para processar sempre d-1
hoje = datetime.now() # data de hoje
ontem = hoje - timedelta(1) # calculando d-1
ontem = ontem.strftime('%Y-%m-%d') # formatando data(d-1) para usar na váriavel 'reportDate'


# validando se a data(d-1) contém informação
# realizando o 'download sales and trends reports'
report = api.download_sales_and_trends_reports(
filters={'frequency': 'DAILY',
        'reportSubType': 'SUMMARY',
        'reportType': 'SALES',
        'vendorNumber': vendor_number,
        'reportDate': ontem}
    ,save_to='/tmp/report.csv')


# lendo o csv para crianção do df
csv = pd.read_csv("/tmp/report.csv", encoding='utf-8', sep='\t', low_memory=False, on_bad_lines='skip', skipinitialspace=True)


# trtando colunas
csv.rename(columns={'Provider Country': 'Provider_Country', 
            'Product Type Identifier': 'Product_Type_Identifier', 
            'Developer Proceeds': 'Developer_Proceeds',
            'Begin Date': 'Begin_Date',
            'End Date': 'End_Date',
            'Customer Currency': 'Customer_Currency',
            'Country Code': 'Country_Code',
            'Currency of Proceeds': 'Currency_of_Proceeds',
            'Apple Identifier': 'Apple_Identifier',
            'Customer Price': 'Customer_Price',
            'Promo Code': 'Promo_Code',
            'Parent Identifier': 'Parent_Identifier',
            'Supported Platforms': 'Supported_Platforms',
            'Proceeds Reason': 'Proceeds_Reason',
            'Preserved Pricing': 'Preserved_Pricing',
            'Order Type': 'Order_Type'}, inplace=True)

print('Processado os dados do dia {}'.format(ontem))


# obtendo id's do BigQuery
id_tabela = "`mosaic-fertilizantes.nutrisafras_apple.app_installations`"
project = id_tabela.split('.')[0]


# incluindo table_schema no BigQuery
table_schema = [{'name': 'Provider','type': 'STRING'},
{'name': 'Provider_Country','type': 'STRING'},
{'name': 'SKU','type': 'STRING'},
{'name': 'Developer','type': 'STRING'},
{'name': 'Title','type': 'STRING'},
{'name': 'Version','type': 'STRING'},
{'name': 'Product_Type_Identifier','type': 'STRING'},
{'name': 'Units','type': 'INTEGER'},
{'name': 'Developer_Proceeds','type': 'FLOAT'},
{'name': 'Begin_Date','type': 'STRING'},
{'name': 'End_Date','type': 'STRING'},
{'name': 'Customer_Currency','type': 'STRING'},
{'name': 'Country_Code','type': 'STRING'},
{'name': 'Currency_of_Proceeds','type': 'STRING'},
{'name': 'Apple_Identifier','type': 'INTEGER'},
{'name': 'Customer_Price','type': 'FLOAT'},
{'name': 'Promo_Code','type': 'FLOAT'},
{'name': 'Parent_Identifier','type': 'FLOAT'},
{'name': 'Subscription','type': 'FLOAT'},
{'name': 'Period','type': 'FLOAT'},
{'name': 'Category','type': 'STRING'},
{'name': 'CMB','type': 'FLOAT'},
{'name': 'Device','type': 'STRING'},
{'name': 'Supported_Platforms','type': 'STRING'},
{'name': 'Proceeds_Reason','type': 'FLOAT'},
{'name': 'Preserved_Pricing','type': 'FLOAT'},
{'name': 'Client','type': 'FLOAT'},
{'name': 'Order_Type','type': 'FLOAT'}]


# inicializando o functions_framework
@functions_framework.http

def result(request):
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    # criando df
    df_report = pd.DataFrame(data=csv)

    res = sf.cria_bq(df_report, id_tabela, 'append', project, csv, table_schema)
    
    if request_json and 'downloads' in request_json:
        downloads = request_json['downloads']
    elif request_args and 'downloads' in request_args:
        downloads = request_args['downloads']
    else:
        return res