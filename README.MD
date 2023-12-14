# Web Scraping of Social Service Project Enrollments

* Developed a Python script to perform web scraping of Tec de Monterrey's official offering of solidarity projects for the Winter 2024 period, allowing more than 17,000 students to check the availability of any program they want to enroll.
* Formulated HTTPS requests to obtain JSON formatted data and analyzed it leveraging libraries to perform regular expression matching and parsing.
* Enabled the visualization of the data by exporting it to a CSV file and simplified its usability by generating an Excel file with a table format through the Pandas and XlsxWriter libraries.

## Updating entries

To see updated data in [`winter2024.xlsx`](./winter2024.xlsx) it is necessary that [`main.py`](./main.py) is run.

## To fix

Cookie revocation