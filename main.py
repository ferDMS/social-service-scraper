"""
Title: Script to Scrape Solidarity Project Registrations for Winter Period
Author: Fernando Daniel Monroy Sáncez
Description: 
    This script performs a POST request to obtain data from a server, 
    processes the response, and writes the information to a CSV and Excel file.
"""

import json
import re
import requests
import pandas as pd

def requestData():
    """
    Request all registrations to solidarity projects for the winter period
    Performs a POST request to obtain the specified data from the server
    Response is a JSON object with the information, formatted in columns and rows
    All the data (the response) is written into an external .csv file
    """

    print("Initializing...")

    # Specify the amount of entries to request from the server
    rows = 10000

    # Request payload which defines the amount of entries to obtain
    payload = {
        "dataRequest": [{
            "requestContext": {
                "reportContext": {
                    "reportId": "1e2031bb-c1c2-4834-8c67-668d432e1a12",
                    "pageId": "p_beb89xjkcd",
                    "mode": 1,
                    "componentId": "cd-fma89xjkcd",
                    "displayType": "simple-table"
                },
                "requestMode": 0
            },
            "datasetSpec": {
                "dataset": [{
                    "datasourceId": "57f0958f-190f-47b9-a937-4efa365d5143",
                    "revisionNumber": 0,
                    "parameterOverrides": []
                }],
                "queryFields": [{
                    "name": "qt_n0b74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_39243438_"}
                }, {
                    "name": "qt_o0b74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_n341627934_"}
                }, {
                    "name": "qt_p0b74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_n1986575415_"}
                }, {
                    "name": "qt_isc74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_1169845789_"}
                }, {
                    "name": "qt_jsc74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_1336977314_"}
                }, {
                    "name": "qt_ksc74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_n1368112855_"}
                }, {
                    "name": "qt_lsc74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_n120571253_"}
                }],
                "sortData": [{
                    "sortColumn": {
                        "name": "qt_o0b74z9kcd",
                        "datasetNs": "d0",
                        "tableNs": "t0",
                        "dataTransformation": {"sourceFieldName": "_n341627934_"}
                    },
                    "sortDir": 0
                }],
                "includeRowsCount": True,
                "relatedDimensionMask": {
                    "addDisplay": False,
                    "addUniqueId": False,
                    "addLatLong": False
                },
                "paginateInfo": {"startRow": 1, "rowsCount": rows},
                "filters": [],
                "features": [],
                "dateRanges": [],
                "contextNsCount": 1,
                "dateRangeDimensions": [{
                    "name": "qt_6be74z9kcd",
                    "datasetNs": "d0",
                    "tableNs": "t0",
                    "dataTransformation": {"sourceFieldName": "_1176618056_",
                                        "transformationConfig": {"transformationType": 5}}
                }],
                "calculatedField": [],
                "needGeocoding": False,
                "geoFieldMask": [],
                "multipleGeocodeFields": []
            },
            "role": "main",
            "retryHints": {"useClientControlledRetry": True, "isLastRetry": False, "retryCount": 0,
                        "originalRequestId": "cd-fma89xjkcd_0_0"}
        }]
    }

    # Request headers that define the necessary cookies and tokens for the request
    headers = {
        'authority': 'lookerstudio.google.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en,es;q=0.9',
        'content-type': 'application/json',
        'cookie': 'RAP_XSRF_TOKEN=AImk1AIFnvoKKIQHi-JggogBFp-OChSXhA:1702543467049; S=billing-ui-v3=-uE5FmQTgRm9c1xNjIS2zITeD6W7M7tM:billing-ui-v3-efe=-uE5FmQTgRm9c1xNjIS2zITeD6W7M7tM; _ga_S4FJY0X3VX=deleted; _gid=GA1.3.527423460.1702490375; AEC=Ackid1S84bCRfTseM8TNfC6CExoZ87VMJ7lK2gV3y0uUTgLO1vIMbRxD5uw; SID=eAh12HppB3sSLqTDfpSj7b6dyvPz_PWk2eR2dfr1LUG8tpqh2VaNnUgIFPgtH5In95Nx3A.; __Secure-1PSID=eAh12HppB3sSLqTDfpSj7b6dyvPz_PWk2eR2dfr1LUG8tpqhF885xpZ-ePhFjsjQEPolLw.; __Secure-3PSID=eAh12HppB3sSLqTDfpSj7b6dyvPz_PWk2eR2dfr1LUG8tpqhafpQImC5zg6ZUeSK9Zobwg.; HSID=AYaSa6TjzuKTa6Qef; SSID=Axp-eSyLDgd8GGx7l; APISID=kY_6tEU6odTYP9H7/AyOM7hV-nqqBGgmcO; SAPISID=-bDcrSg_hcr-JH-k/AY584QkLdhxYFyFbF; __Secure-1PAPISID=-bDcrSg_hcr-JH-k/AY584QkLdhxYFyFbF; __Secure-3PAPISID=-bDcrSg_hcr-JH-k/AY584QkLdhxYFyFbF; __Secure-ENID=16.SE=VO_1BsJ8OivF4zQrncEva2HMGKKe7Y_1K26Z3GonD85ZTNsw_i4TFkMfO7ZNAq8qhOkINwG08BwT1JBVn3nlseYRTKYHT4w6zkAfKw-NmbrEzrKxFofGLBf6xXrJOQBDKCTL-HH8yeurrO0zY88y8NS7nrdyWx40rEAouD2ooR8IAwQphuawbdCpJD22aJrXZznzxrzGKLPBIRUS5sFyVLWa1pZluBSYXWQc3T_layw0meMbTTHjsCwxQL8f-u0KW7x_saxi1ikrUowIIvNQeBW7RSdVWDlnsgAdiNO8ZeNxUQpycEpazcDr1dnPE0gCpQBZQdjLYPTiX3cWXnVinD3_LZKd749jQVy4-KUH32cC; 1P_JAR=2023-12-14-08; _ga=GA1.3.711671726.1700965214; _ga_S4FJY0X3VX=GS1.1.1702541928.22.1.1702543467.0.0.0; NID=511=k1u-NymQIJGWGWs8xQgKmVSAL_ZS2GEC8vSwwT3B4EVizi3LU6PEG12YYn_HefCeB2WgLghGVc-rLHwIFA0JlMZmHEi-z-zQ1bl7ov_9mH5R-SExgWY3tUdHHhkddl_8gkU5MdCJo3uTe3raKHEfv5uKYvPjHlLg9baHSbcjheTvziH1_OS6VOatwr8BnD81wtDzzDtF22zwRQF-KhAM1yQV2KvIZv2c90rQrVmNiDkB8xowwhDasGIt4rcXRu4_FIzoZk3Ng9RLNd_HhJclpXnW7I65o087i1tUBvFhrrhgIIsdaReGorifaR_4PPWFY_MMaklvK5dIHiMa1KQfrFw44vcTjPV0tuwq3fl8pzSbD9bQp5EMwc81BwRwP67S4fMx6xavzmIqbPWBeAlQ7bEDtVTnpJvrDR3ysaHfKfJt0BytnxKHDcS22xA-jdO-LooC7QCPdh3HQvI86cbprrMRQyGddTqJIA0AiHMor-vKVIsX8ZpZKCoowrBBi9SrWnQkYRp_7yuoCPqTklrBrjLfxxPlpexFecmiQgklp51gQB8CW1lWUSjZTGB-bFy4OZPYe9aysZLsiW5tOKAMp1na7q7x_O6f0T4ShGhPuinrOp9rM-y5RnYDBNDSwwfbJBsAvLnn9QY; SIDCC=ABTWhQHOYxPyhPO5LYo18-bIme1un0nJ6SYHhiBzvvEzcGgjomsWSP_4D5AvVQGx1-DQfdJskg; __Secure-1PSIDCC=ABTWhQEygL291cj9FmwDrBrPsvb1H9buWRPqOMSsJqcXq2Y67V7Sb32QQDpzyLOtEet9UhxkW3A; __Secure-3PSIDCC=ABTWhQG7VoXV5GTqyaloNrUO-t75x3-1DXqYm4fx67B99zoQYYPHjrV2ZtuLv00XGKjvsAtLVXo',
        'encoding': 'null',
        'origin': 'https://lookerstudio.google.com',
        'referer': 'https://lookerstudio.google.com/u/0/reporting/1e2031bb-c1c2-4834-8c67-668d432e1a12/page/p_beb89xjkcd?s=sIOHo74qPCY',
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'x-client-data': 'CJa2yQEIpLbJAQipncoBCKXbygEIlaHLAQib/swBCIagzQEI3L3NAQjf6c0BCN7rzQEI3+zNARiPzs0BGKfqzQE=',
        'x-rap-xsrf-token': 'AImk1AIFnvoKKIQHi-JggogBFp-OChSXhA:1702543467049'
    }

    # URL which with the connection is stablished
    url = "https://lookerstudio.google.com/u/0/batchedDataV2?appVersion=20231212_1000"

    # Send the request through a POST call and get reponse from server
    print(f"Requesting up to {rows} entries...")
    response = requests.post(url, json=payload, headers=headers)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Write the response content to a file
        with open("response.txt", "w", encoding="utf-8") as file:
            file.write(response.text)
        print("Successfully retrieved response from server")
        print("Response saved to response.txt")
    else:
        print(f"Error: {response.status_code} - {response.text}")


def parseData_csv(file_name):
    """
    The response is ran through a regex and parsing library to obtain the target information
    The table is reconstructed in a CSV type file by performing a loop for each row obtained
    """

    # File name for .csv and .xlsx files

    # Read the response from file and obtain only the JSON object
    with open("response.txt", "r", encoding="utf-8") as file:
        response = file.read()
        print("Searching for matching JSON object...")
        json_data = re.findall('(\{.*$)', response) 
        print("JSON found, parsing the data...")

    # Parse the JSON response
    parsed_json = json.loads(json_data[0])

    # Access the columns of the relevant data
    columns = parsed_json['dataResponse'][0]['dataSubset'][0]['dataset']['tableDataset']['column']

    # Number of rows to visualize (not including headers)
    n = len(columns[0]['stringColumn']['values'])
    print(f"Data parsed, found {n} entries")

    # Initialize text variable with headers
    text = "Periodo,Matrícula,Nombre,Institución,Nombre del proyecto,Duración,Modalidad\r\n"
    print("Preparing .csv file...")

    # Scrap data and prepare it for CSV
    for row in range(0, n):
        for column in columns:
            text += '"' + column['stringColumn']['values'][row] + '"' + ","
        text = text[:len(text)-1]
        text += "\r\n"

    # Export formatted data to CSV
    with open(f"{file_name}.csv", "w", encoding="utf-8") as file:
        file.write(text)
    print(f"Data saved to {file_name}.csv")


def exportData_xlsx(file_name):
    """
    The CSV file is exported into an Excel type file (.xlsx)
    An ExcelWriter object is initialized to configure the sheet
    The table format is inserted so that the user can filter and sort the data
    Every column is extended to fit its content and the file is saved
    """

    # Read .csv file using pandas
    print("Preparing .xlsx file...")
    df = pd.read_csv(f"{file_name}.csv")

    # Export formatted data to an excel file (.xlsx)
    writer = pd.ExcelWriter(f'{file_name}.xlsx', engine='xlsxwriter') 
    df.to_excel(writer, sheet_name=f"{file_name}", index=None, header=True)

    # Declare instance of sheet as the active worksheet (to insert table)
    ws = writer.sheets[f"{file_name}"]

    # Get column names from DataFrame object and prepare them for table creation
    # as dictionaries with a 'header' key as the column name
    col_names = []
    for col_name in df.columns:
        col_names.append({'header' : col_name, 'text_wrap': True})

    # Get size of table (with headers) as (# of rows, # of columns)
    size = {
        'rows' : df.shape[0]-1,
        'columns' : df.shape[1]-1
    }

    # Create header objects as
    ws.add_table(0, 0, size['rows'], size['columns'], {
        'columns' : col_names,
        'header_row' : True,
        'banded_rows': True,
        'style' : "Table Style Medium 2"
    })

    # Autofit column widths
    ws.autofit()

    # Save writer object and create Excel file
    writer.save()
    print(f"Data saved to {file_name}.xlsx")


file_name = "winter2024"
requestData()
parseData_csv(file_name)
exportData_xlsx(file_name)