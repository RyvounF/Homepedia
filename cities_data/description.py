import csv
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient('mongodb+srv://admin:Agudib24@citydata.1uvq9yv.mongodb.net/')
db = client['Cities']  # Создание или подключение к базе данных
collection = db['cities']  # Создание или подключение к коллекции

# Чтение данных из CSV файла с деталями городов и обновление соответствующих документов в MongoDB
with open('cities_details.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        city_code = row['city_code']
        collection.update_one({'city_code': city_code}, {'$set': {'description': row}})

print("Данные успешно обновлены в MongoDB.")
