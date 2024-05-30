import requests
import json
from statistics import mean
from requests.auth import HTTPBasicAuth
import csv
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s -%(message)s', level=logging.INFO)

# Set credentials for Grafana authorization
USERNAME = os.environ.get("USERNAME")
PASSWORD = os.environ.get("PASSWORD")

# Set path to Root CA certificate file
ROOT_CA_CERT = "./certificates/rootca.crt"

def get_metrics(expr, period):
    expr = expr
    # Set Grafana URL 
    URL = 'https://dev-grafana.lan/api/ds/query'
    basic_auth = HTTPBasicAuth(USERNAME,PASSWORD)
    query_headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    payload = {
        "queries": [
            {
                "refId":"A",
                "datasource":{
                    "type":"prometheus",
                    # Set data source UID
                    "uid":"PBFA97CFB590B2093"
                },
                "format":"table",
                "expr":expr
            }
        ],
        "from":f"now-{period}h",
        "to":"now"
    }
    logger.info("Sending request")
    response = requests.post(URL, headers=query_headers, data=json.dumps(payload), auth=basic_auth, verify=ROOT_CA_CERT)

    return response 

def parse_metrics(ris_code, mnemocode, response):
    ris_code = ris_code
    mnemocode = mnemocode
    logger.info(response.json())
    with open(f'{ris_code}_{mnemocode}_Cap.csv', 'a', newline='') as file:
        writer = csv.writer(file, quotechar="'")
        for frame in response.json()['results']['A']['frames']:
            writer.writerow([frame['schema']['name'], frame['data']['values'][0][-1], max(frame['data']['values'][1]), mean(frame['data']['values'][1])])

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    expressions_filepath = './expressions.csv'
    expressions = []
    with open(expressions_filepath, 'r') as csv_file:
        lines = csv.reader(csv_file, delimiter = ';')
        for line in lines:
            expressions.append(line)
 
    for expression in expressions
        logger.info(f'Request {expression[2]}')
        response = get_metrics(expression[2], 6)
        logger.info(f'Request {expression[2]} was successful')

        logger.info(f'Parsing metrics for {expression[0]}_{expression[1]}')
        parse_metrics(expression[0], expression[1], response)
        logger.info(f'Parsing was successful for {expression[0]}_{expression[1]}')