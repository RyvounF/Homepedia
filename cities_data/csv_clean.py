import csv
import re


with open('cities.csv', 'r', encoding='utf-8') as csvfile, \
        open('cities_data_cleaned.csv', 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(csvfile)
    next(reader)
    writer = csv.writer(outfile)
    writer.writerow(['city_code'])

    for row in reader:
        insee_code = row[0]
        city_code = row[1]
        city = '-'.join([re.sub(r'\bst\b', 'saint', re.sub(r'\bste\b', 'sainte', cell)) for cell in city_code.split()])
        new_city_data = f"{city}-{insee_code}"
        writer.writerow([new_city_data])