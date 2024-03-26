import csv
from pymongo import MongoClient

# Подключение к MongoDB
client = MongoClient('mongodb+srv://admin:Agudib24@citydata.1uvq9yv.mongodb.net/')
db = client['Cities']  # Создание или подключение к базе данных
collection = db['cities']  # Создание или подключение к коллекции

# Определение модели документа для города
def create_city_document(city_code):
    description = {
        'city_code': city_code,
        'name': None,
        'presentation_title': None,
        'presentation_body': None,
        'population': {
            'residents': None,
            'years_range': None,
            'residents_km': None,
            'median_age': None,
            'age_distribution': {
                '0_14_y_o': None,
                '15_29_y_o': None,
                '30_44_y_o': None,
                '45_59_y_o': None,
                '60_74_y_o': None,
                '75_89_y_o': None,
                '90_+_y_o': None
            },
            'degree_level': {
                'no_diploma': None,
                'college_certificate': None,
                'cap_bep': None,
                'baccalaureate': None,
                'bac2_bac4': None,
                'bac3_bac4': None,
                'bac5_bac+': None
            },
        },
        'climate': {
            'climate_description': None,
            'median_t': None,
            'max_t': None,
            'min_t': None,
            'median_warm': None,
            'median_rain': None,
            'median_cold': None
        },
        'economy': {
            'economy_title': None,
            'revenu_title': None,
            'revenu_value': None,
            'chomage_title': None,
            'chomage_value': None,
            'fibre_title': None,
            'fibre_value': None,
            'entreprises_title': None,
            'entreprises_value': None
        },
        'estate': {
            'estate_title': None,
            'median_price_m2_apartment': None,
            'median_price_m2_mansion': None,
            'habitat_type_apartment': None,
            'habitat_type_apartment_value': None,
            'habitat_type_mansion': None,
            'habitat_type_mansion_value': None,
            'estate_use': {
                'vacant_housing': None,
                'main_residences': None,
                'secondary_residences': None
            },
            'number_of_rooms': {
                'one': None,
                'two': None,
                'three': None,
                'four': None,
                'five': None
            },

        },
        'security': {
           'security_title': None,
           'security_number': None,
           'security_description': None,
           'crimes_type': {
               'burglaries': None,
               'auto_thefts': None,
               'private_thefts': None,
               'physical_violence': None,
               'sex_violence': None
           },
           'crimes_national': {
               'burglaries_national': None,
               'auto_thefts_national': None,
               'private_thefts_national': None,
               'physical_violence_national': None,
               'sex_violence_national': None
           },
           'crimes_local': {
               'burglaries_local': None,
               'auto_thefts_local': None,
               'private_thefts_local': None,
               'physical_violence_local': None,
               'sex_violence_local': None
           },
        },
        'policy': {
            'policy_title': None,
            'policy_description': None,
            'year_1': {
                'year': None,
                'candidate_winner': None,
                'winner_value': None,
                'candidate_looser': None,
                'looser_value': None
                       },
            'year_2': {
                'year': None,
                'candidate_winner': None,
                'winner_value': None,
                'candidate_looser': None,
                'looser_value': None
                       },
            'year_3': {
                'year': None,
                'candidate_winner': None,
                'winner_value': None,
                'candidate_looser': None,
                'looser_value': None
                       },
            'year_4': {
                'year': None,
                'candidate_winner': None,
                'winner_value': None,
                'candidate_looser': None,
                'looser_value': None
                       },
        },
        'services': {
            'services_title': None,
            'services_types': {
                'health': {
                    'service_title': None,
                    'doctor': None,
                    'doctor_value': None,
                    'dentist': None,
                    'dentist_value': None,
                    'nurse': None,
                    'nurse_value': None,
                    'pharmacy': None,
                    'pharmacy_value': None,
                    'emergencies': None,
                    'emergencies_value': None
                },
                'education': {
                    'service_title': None,
                    'nursery': None,
                    'nursery_value': None,
                    'kindergarten': None,
                    'kindergarten_value': None,
                    'elementary_school': None,
                    'elementary_school_value': None,
                    'college': None,
                    'college_value': None,
                    'high_school': None,
                    'high_school_value': None
                 },
                'shops': {
                    'service_title': None,
                    'hypermarket': None,
                    'hypermarket_value': None,
                    'supermarket': None,
                    'supermarket_value': None,
                    'grocery_store': None,
                    'grocery_store_value': None,
                    'bakery': None,
                    'bakery_value': None,
                    'gas_station': None,
                    'gas_station_value': None,
                    'post_office': None,
                    'post_office_value': None

                },
                'transport_hobbies': {
                    'service_title': None,
                    'hotel': None,
                    'restaurant': None,
                    'cinema': None,
                    'library': None,
                    'SNCF_train_station': None,
                    'airport': None
                },
            }
            },
        'city_hall':{
            'city_hall_name': None,
            'city_hall_info': {
                'address': None,
                'phone': None,
                'email': None,
                'hours': None,
            }
        }
            }
    return {
        'city_code': city_code,
        'description': description  # Добавляем поле "description" в документ
    }

# Чтение данных из CSV файла и импорт в MongoDB
with open('cities_data_cleaned.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Пропуск заголовка

    for row in reader:
        city_code = row[0]
        city_document = create_city_document(city_code)

        # Вставка документа в коллекцию MongoDB
        collection.insert_one(city_document)


print("Данные успешно импортированы в MongoDB.")