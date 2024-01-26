"""
Title: Script to Scrape Solidarity Project Registrations for Winter Period
Author: Fernando Daniel Monroy Sánchez
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

    # Request cookies that define the necessary tokens to access the information
    cookies = {
    "NID": "511=TJtaE6hIQ5rTW0K77jRvDIjbcoZlMvq1vXs0YbrF8ePc8anb__R6zQHrtSFLYFPIdHL9S1dZYgkNw_w3DB32-R3UmYI1wOmqti-_NOAQKQamGaiz8FvO5eJwkq00E0IQKXUMr4dcjGCZCNa63iMS9enKCJvw-abBe4orPkYSZo0",
    "_gid": "GA1.3.1078430438.1706232920",
    "_ga": "GA1.3.826474088.1706232920",
    "_gat_marketingTracker": "1",
    "_gat": "1",
    "_ga_S4FJY0X3VX": "GS1.1.1706232920.1.0.1706232922.0.0.0",
}

    # Request headers with information about the request and the client
    headers = {
    "authority": "lookerstudio.google.com",
    "method": "POST",
    "path": "/batchedDataV2?appVersion=20240123_0800",
    "scheme": "https",
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en",
    "content-length": "1997",
    "content-type": "application/json",
    #"cookie": "NID=511=TJtaE6hIQ5rTW0K77jRvDIjbcoZlMvq1vXs0YbrF8ePc8anb__R6zQHrtSFLYFPIdHL9S1dZYgkNw_w3DB32-R3UmYI1wOmqti-_NOAQKQamGaiz8FvO5eJwkq00E0IQKXUMr4dcjGCZCNa63iMS9enKCJvw-abBe4orPkYSZo0; _gid=GA1.3.1078430438.1706232920; _ga=GA1.3.826474088.1706232920; _gat_marketingTracker=1; _gat=1; _ga_S4FJY0X3VX=GS1.1.1706232920.1.0.1706232922.0.0.0",
    "encoding": "null",
    "origin": "https://lookerstudio.google.com",
    "referer": "https://lookerstudio.google.com/reporting/1e2031bb-c1c2-4834-8c67-668d432e1a12/page/p_povubyjdcd?s=sIOHo74qPCY%5C",
    "sec-ch-ua": "\"Not_A Brand\";v=\"8\", \"Chromium\";v=\"120\", \"Google Chrome\";v=\"120\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

    # # Request parameters (query parameters that could also be specified in the URL)
    params = {
        'appVersion': '20231211_0700',
    }

    # Request JSON data or payload which specified the amount of rows to obtain
    json_data = {"dataRequest":[{"requestContext":{"reportContext": {"reportId":"1e2031bb-c1c2-4834-8c67-668d432e1a12","pageId":"p_povubyjdcd","mode":1,"componentId":"cd-108tm1jdcd","displayType":"simple-table"},"requestMode":0},"datasetSpec":{"dataset":[{"datasourceId":"d68ea43a-2f0e-4a00-8898-22f8a47ca430","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_fh7tm1jdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_985914030_"}},{"name":"qt_utxp42jdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1124254750_"}},{"name":"qt_4ai252jdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n414281798_"}},{"name":"qt_8ygnj7jdcd","datasetNs":"d0","tableNs":"t0","resultTransformation":{"analyticalFunction":0,"isRelativeToBase":False},"dataTransformation":{"sourceFieldName":"_n179593893_"}},{"name":"qt_7328s6jdcd","datasetNs":"d0","tableNs":"t0","resultTransformation":{"analyticalFunction":0,"isRelativeToBase":False},"dataTransformation":{"sourceFieldName":"_302064841_"}},{"name":"qt_0qpj82jdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1735679163_"}},{"name":"qt_2rppq3jdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n920199899_"}},{"name":"qt_znceizkdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1927870623_"}}],"sortData":[{"sortColumn":{"name":"qt_fh7tm1jdcd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_985914030_"}},"sortDir":1}],"includeRowsCount":True,"relatedDimensionMask":{"addDisplay":False,"addUniqueId":False,"addLatLong":False},"paginateInfo":{"startRow":1,"rowsCount":rows},"filters":[],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":False,"geoFieldMask":[],"multipleGeocodeFields":[]},"role":"main","retryHints":{"useClientControlledRetry":True,"isLastRetry":False,"retryCount":0,"originalRequestId":"cd-108tm1jdcd_0_0"}}]}

    # URL for the connection
    url = "https://lookerstudio.google.com/batchedDataV2"

    # Send the request through a POST call and get reponse from server
    print(f"Requesting up to {rows} entries...")
    response = requests.post(
        url,
        params=params,
        cookies=cookies,
        headers=headers,
        json=json_data,
    )

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
        json_data = re.findall(r'(\{.*$)', response) 
        print("JSON found, parsing the data...")

    # Parse the JSON response
    parsed_json = json.loads(json_data[0])

    # Access the columns of the relevant data
    columns = parsed_json['dataResponse'][0]['dataSubset'][0]['dataset']['tableDataset']['column']

    # Number of rows to visualize (not including headers)
    n = parsed_json['dataResponse'][0]['dataSubset'][0]['dataset']['tableDataset']['totalCount']
    print(f"Data parsed, found {n} entries")

    # Initialize text variable with headers
    text = "Periodo,Matrícula,Nombre,Semestre,Carrera,Institución,Proyecto,Duración\n"
    print("Preparing .csv file...")

    # Scrap data and prepare it for CSV
    idx = {i : 0 for i in range(len(columns))}
    for row in range(n):
        for i, column in enumerate(columns):
            if row in column['nullIndex']:
                text += '"",'
            else:
                text += '"' + column['stringColumn']['values'][idx[i]] + '"' + ","
                idx[i] += 1
        text = text[:len(text)-1]
        text += "\n"

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
    with pd.ExcelWriter(f'{file_name}.xlsx', engine='xlsxwriter') as writer:
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
            'rows' : df.shape[0],
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

    print(f"Data saved to {file_name}.xlsx")


file_name = "feb_jun_2024"
requestData()
parseData_csv(file_name)
exportData_xlsx(file_name)