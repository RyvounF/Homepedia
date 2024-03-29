import csv
import re

from unidecode import unidecode

with open('cities.csv', 'r', encoding='utf-8') as csvfile, \
        open('cities_data_region.csv', 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(csvfile)
    next(reader)
    writer = csv.writer(outfile)
    writer.writerow(['region_name','department_name','department_number','city','insee_code',])

    for row in reader:
        insee_code = row[0]
        city_code = row[1]
        region_name = row[8]
        department_number = row[7]
        department_name = row[6]
        city = '-'.join([re.sub(r'\bst\b', 'saint', re.sub(r'\bste\b', 'sainte', cell)) for cell in city_code.split()])
        regions = unidecode(region_name)
        departments = unidecode(department_name)
        writer.writerow([regions,departments,department_number,city,insee_code])