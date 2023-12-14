# Web Scraping of Social Service Project Enrollments

* Developed a **Python** script to perform **web scraping** of Tec de Monterrey's official offering of solidarity projects for the Winter 2024 period, allowing more than **17,000** students to check the availability of any program they want to enroll.
* Designed **HTTPS** requests and manipulated **JSON** responses by leveraging libraries to perform regular expression matching and parsing.
* Enabled the visualization of the data by formatting it as CSV and simplified its usability by generating a table formatted Excel file through the **Pandas** and **XlsxWriter** libraries.

The script generates a text file with the raw response from the server [`response.txt`](./response.txt) and then parses it to obtain the JSON object inside it. After parsing, the data is rearranged and formatted into [`winter2024.csv`](./winter2024.csv). Finally, the information is displayed in a table format that can be filtered and sorted by creating an Excel file [`winter2024.xlsx`](./winter2024.xlsx) that can be easily manipulated by any user.

## Dependencies

A list of dependencies can be found in [`requirements.txt`](./requirements.txt)

## Updating entries

In order to update the data found on the Excel file [`winter2024.xlsx`](./winter2024.xlsx) the main web scraping script [`main.py`](./main.py) should be run.