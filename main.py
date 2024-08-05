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
import csv

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
        "SID": "g.a000mQh12OO7ZYVcK1lC75bs0VxgU9ywKiNx1etr1x0KkgiP9ba-Xkthb9BbWyu7pmDsfClXOwACgYKAeUSARMSFQHGX2Mi3RfPYyXuvf3o1xlVRBRFrBoVAUF8yKoJOVefcRjr3ihkUwfBr-pk0076",
        "__Secure-1PSID": "g.a000mQh12OO7ZYVcK1lC75bs0VxgU9ywKiNx1etr1x0KkgiP9ba-pyusIO-Qryi8yYDfkrOrYgACgYKAboSARMSFQHGX2MiksbvAzfTqtpTBbgv2DlwjxoVAUF8yKq7R9YT0GFKcVVgwYkYSuyL0076",
        "__Secure-3PSID": "g.a000mQh12OO7ZYVcK1lC75bs0VxgU9ywKiNx1etr1x0KkgiP9ba-B_FLB6H5sa86l95_TcIiOwACgYKAXQSARMSFQHGX2MiLn_vGwPBopxF6s8SR_F18RoVAUF8yKqYWNaxcpA7k7ERYLB38QBn0076",
        "HSID": "Ak-J08Gu5RXzrMXxl",
        "SSID": "AVoBE8V592CUj81_C",
        "APISID": "ZiiZ1bUeU8qSCG92/A7qMHsHtgI9-9cJeR",
        "SAPISID": "pJtGNDbQNcgV8yvJ/AGLc0VLmItPjp5gG7",
        "__Secure-1PAPISID": "pJtGNDbQNcgV8yvJ/AGLc0VLmItPjp5gG7",
        "__Secure-3PAPISID": "pJtGNDbQNcgV8yvJ/AGLc0VLmItPjp5gG7",
        "AEC": "AVYB7cpPFNrr3JDi0RXCcSZrvNMurqqz4i82OocYGzJ9lYPlwhfdBROCgQ",
        "NID": "516=PSZWxYkm4E4nSQmVWiTurBvwp88LhCta8WyLD7Oc3wn6MHafKTwgeLOY0xrrATE2jKJvu9kBIqc4UbzNyZY3CoZ8a2gApKp6L1YqkAkEry9wAaIyIc54-gSGhEMNek-RgzmCWonsSN_MNpJTWWC3WTsqTAUDO9BZ1aYqpkWLD15MFcxRnjcAfi24ARIheMEhOTLae1WV88NR8xTepyVXMVy2R23Aqi9_wQytIT_SLQk-NoTnBoJrDIo-ifewBYxFN6VUm5pGgQbrhxr7XAtVefhAjPfzcJMCM5KelUD3pZwb_ocAGPolC63qNHlQtLleBCHA90aInPrKLtFu0zdkMgA6xJuxyWmZ78Lv6Rffth2AQW1lHkDq_VwpTnYPTIzpR3BFJb2UnOk",
        "__Secure-1PSIDTS": "sidts-CjEB4E2dkSF3vKmH1bM0gZThHEWNxK_ZW3dx807YS8W969I9Tu347Pe-TXlIlFysD52UEAA",
        "__Secure-3PSIDTS": "sidts-CjEB4E2dkSF3vKmH1bM0gZThHEWNxK_ZW3dx807YS8W969I9Tu347Pe-TXlIlFysD52UEAA",
        "RAP_XSRF_TOKEN": "AImk1AK59lPU5zwIqr62_rnv_GEirWL4nQ:1722892103601",
        "SIDCC": "AKEyXzXGYqQss-pYGGXWDS2wfi7t08GElhnZYeLEFQB7sudSBT_iQyDTo1mnqtdZZcaL7uIyCRU",
        "__Secure-1PSIDCC": "AKEyXzVm-yNHo839lZohmajbOXEKfQohUidwCOAT0brRer7-VyuUFgaPF0jPvPqjbc0V441VWZE",
        "__Secure-3PSIDCC": "AKEyXzXzVHGvEJK3gNGDl_tt80TCFcoj94upG2bKXbLrcHM3F-eNhDGlYuq76iZLFke5164jaDU"
    }


    # Request headers with information about the request and the client
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,es;q=0.8,es-MX;q=0.7',
        'content-type': 'application/json',
        # 'cookie': 'SID=g.a000mQh12OO7ZYVcK1lC75bs0VxgU9ywKiNx1etr1x0KkgiP9ba-Xkthb9BbWyu7pmDsfClXOwACgYKAeUSARMSFQHGX2Mi3RfPYyXuvf3o1xlVRBRFrBoVAUF8yKoJOVefcRjr3ihkUwfBr-pk0076; __Secure-1PSID=g.a000mQh12OO7ZYVcK1lC75bs0VxgU9ywKiNx1etr1x0KkgiP9ba-pyusIO-Qryi8yYDfkrOrYgACgYKAboSARMSFQHGX2MiksbvAzfTqtpTBbgv2DlwjxoVAUF8yKq7R9YT0GFKcVVgwYkYSuyL0076; __Secure-3PSID=g.a000mQh12OO7ZYVcK1lC75bs0VxgU9ywKiNx1etr1x0KkgiP9ba-B_FLB6H5sa86l95_TcIiOwACgYKAXQSARMSFQHGX2MiLn_vGwPBopxF6s8SR_F18RoVAUF8yKqYWNaxcpA7k7ERYLB38QBn0076; HSID=Ak-J08Gu5RXzrMXxl; SSID=AVoBE8V592CUj81_C; APISID=ZiiZ1bUeU8qSCG92/A7qMHsHtgI9-9cJeR; SAPISID=pJtGNDbQNcgV8yvJ/AGLc0VLmItPjp5gG7; __Secure-1PAPISID=pJtGNDbQNcgV8yvJ/AGLc0VLmItPjp5gG7; __Secure-3PAPISID=pJtGNDbQNcgV8yvJ/AGLc0VLmItPjp5gG7; AEC=AVYB7cpPFNrr3JDi0RXCcSZrvNMurqqz4i82OocYGzJ9lYPlwhfdBROCgQ; NID=516=PSZWxYkm4E4nSQmVWiTurBvwp88LhCta8WyLD7Oc3wn6MHafKTwgeLOY0xrrATE2jKJvu9kBIqc4UbzNyZY3CoZ8a2gApKp6L1YqkAkEry9wAaIyIc54-gSGhEMNek-RgzmCWonsSN_MNpJTWWC3WTsqTAUDO9BZ1aYqpkWLD15MFcxRnjcAfi24ARIheMEhOTLae1WV88NR8xTepyVXMVy2R23Aqi9_wQytIT_SLQk-NoTnBoJrDIo-ifewBYxFN6VUm5pGgQbrhxr7XAtVefhAjPfzcJMCM5KelUD3pZwb_ocAGPolC63qNHlQtLleBCHA90aInPrKLtFu0zdkMgA6xJuxyWmZ78Lv6Rffth2AQW1lHkDq_VwpTnYPTIzpR3BFJb2UnOk; __Secure-1PSIDTS=sidts-CjEB4E2dkSF3vKmH1bM0gZThHEWNxK_ZW3dx807YS8W969I9Tu347Pe-TXlIlFysD52UEAA; __Secure-3PSIDTS=sidts-CjEB4E2dkSF3vKmH1bM0gZThHEWNxK_ZW3dx807YS8W969I9Tu347Pe-TXlIlFysD52UEAA; RAP_XSRF_TOKEN=AImk1AK59lPU5zwIqr62_rnv_GEirWL4nQ:1722892103601; SIDCC=AKEyXzXGYqQss-pYGGXWDS2wfi7t08GElhnZYeLEFQB7sudSBT_iQyDTo1mnqtdZZcaL7uIyCRU; __Secure-1PSIDCC=AKEyXzVm-yNHo839lZohmajbOXEKfQohUidwCOAT0brRer7-VyuUFgaPF0jPvPqjbc0V441VWZE; __Secure-3PSIDCC=AKEyXzXzVHGvEJK3gNGDl_tt80TCFcoj94upG2bKXbLrcHM3F-eNhDGlYuq76iZLFke5164jaDU',
        'dnt': '1',
        'encoding': 'null',
        'origin': 'https://lookerstudio.google.com',
        'priority': 'u=1, i',
        'referer': 'https://lookerstudio.google.com/reporting/1e2031bb-c1c2-4834-8c67-668d432e1a12/page/p_7u1eltbgjd?s=sIOHo74qPCY',
        'sec-ch-ua': '"Chromium";v="127", "Not)A;Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'x-client-data': 'CJ7zygE=',
        'x-rap-xsrf-token': 'AImk1AK59lPU5zwIqr62_rnv_GEirWL4nQ:1722892103601'
    }


    # # Request parameters (query parameters that could also be specified in the URL)
    params = {
        'appVersion': '20231211_0700',
    }

    # Request JSON data or payload which specifies the amount of rows to obtain
    json_data = {"dataRequest":[{"requestContext":{"reportContext":{"reportId":"1e2031bb-c1c2-4834-8c67-668d432e1a12","pageId":"p_7u1eltbgjd","mode":1,"componentId":"cd-a30eltbgjd","displayType":"simple-table"},"requestMode":0},"datasetSpec":{"dataset":[{"datasourceId":"16f933e9-62ff-4729-9d24-774ffa04b898","revisionNumber":0,"parameterOverrides":[]}],"queryFields":[{"name":"qt_5767v8bgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1169845789_"}},{"name":"qt_k4ta7acgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1336977314_"}},{"name":"qt_ixwccbcgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n341627934_"}},{"name":"qt_s1bxjbcgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_1272339798_"}},{"name":"qt_4gxqmbcgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1986575415_"}},{"name":"qt_ur9uqbcgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1368112855_"}},{"name":"qt_5237wccgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_859073000_"}},{"name":"qt_9fuczccgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n1223991429_"}},{"name":"qt_dltl1ccgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n518433374_"}}],"sortData":[{"sortColumn":{"name":"qt_ixwccbcgjd","datasetNs":"d0","tableNs":"t0","dataTransformation":{"sourceFieldName":"_n341627934_"}},"sortDir":0}],"includeRowsCount":True,"relatedDimensionMask":{"addDisplay":False,"addUniqueId":False,"addLatLong":False},"paginateInfo":{"startRow":1,"rowsCount":rows},"dsFilterOverrides":[],"filters":[],"features":[],"dateRanges":[],"contextNsCount":1,"calculatedField":[],"needGeocoding":False,"geoFieldMask":[],"multipleGeocodeFields":[]},"role":"main","retryHints":{"useClientControlledRetry":True,"isLastRetry":False,"retryCount":0,"originalRequestId":"cd-a30eltbgjd_0_0"}}],"requestModifications":[]}

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
    # Initialize list of rows with headers
    rows = [["Socioformador",  "Experiencia",  "Matrícula",  "Carrera",  "Nombre",  "Duración autorizada",  "Responsable de proyecto",  "Correo de responsable",  "Horas acreditables"]]
    print("Preparing .csv file...")
    # Scrap data and prepare it for CSV
    idx = {i : 0 for i in range(len(columns))}
    for row in range(n):
        row_data = []
        for i, column in enumerate(columns):
            if row in column['nullIndex']:
                row_data.append("")
            else:
                row_data.append(column['stringColumn']['values'][idx[i]])
                idx[i] += 1
        rows.append(row_data)
    # Export formatted data to CSV using csv module
    with open(f"{file_name}.csv", "w", encoding="utf-8", newline='') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(rows)
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
    df = pd.read_csv(f"{file_name}.csv", quotechar='"')

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


file_name = "ago_dic_2024"
# requestData()
parseData_csv(file_name)
exportData_xlsx(file_name)