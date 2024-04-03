import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import csv
from pymongo import MongoClient
import re


options = Options()
options.add_argument('headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)
url = "https://www.villesavivre.fr"
def scrape_data(reader, collection, total_requests, last_row_processed):
    for row in reader:
        if total_requests >= 500:
            print(f"{total_requests} requests exceeded")
            time.sleep(60)
            total_requests = 0

        city_code = row[0]
        user_agent = UserAgent().random
        headers = {'User-Agent': user_agent}
        response = requests.get(f'{url}/{city_code}', headers=headers)
        driver.implicitly_wait(15)
        time.sleep(3)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            city_element = soup.find('div', {'class': 'header-content'})

            if city_element:
                # City Description
                name = soup.find('div', {'class': 'header-content'}).find('h1').text.strip()
                presentation = soup.find('section', {'id': 'presentation'})
                presentation_title = presentation.find('h2').text.strip() if presentation else ''
                presentation_body = soup.find('div', {'class': 'dynamic-content'}).text.strip() if soup.find('div', {
                    'class': 'dynamic-content'}) else ''
                # Population
                population = soup.find('section', {'id': 'population'})
                populations = soup.find('div', {'class': 'info-demographie'})
                population_graphs = population.find_all('div', {'class': 'graph-cont'})
                population_title = population.find('h2').text.strip()
                population_elements = populations.find_all('div', {'class': 'demo-content'})
                if len(population_elements) >= 4:
                    residents = population_elements[0].find_all('p')[1].text.strip()
                    residents_value = int(population_elements[0].find_all('p')[0].text.replace(' ', '').strip())
                    years_range = population_elements[1].find('div', {'class': 'demo-flex'}).find_all('p')[1].text.strip()
                    years_range_value = float(population_elements[1].find('div', {'class': 'demo-flex'}).find_all('p')[0].text.replace('%','').replace(',', '.').strip())
                    residents_km = population_elements[2].find_all('p')[1].text.strip()
                    residents_km_value = int(population_elements[2].find_all('p')[0].text.replace(' ', '').replace('\u202f','').strip())
                    median_age = population_elements[3].find('div', {'class': 'demo-flex'}).find_all('p')[1].text.strip()
                    median_age_value = population_elements[3].find('div', {'class': 'demo-flex'}).find_all('p')[0].text.strip()
                if len(population_graphs) >= 2:
                    # Age
                    age_distribution_title = population_graphs[0].find('h4').text.strip()
                    age = population_graphs[0].find_all('div', {'class': 'bar'})
                    from_0_to_14_y_o = age[0].find('p').text.strip()
                    from_0_to_14_y_o_value = float(age[0].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    from_15_to_29_y_o = age[1].find('p').text.strip()
                    from_15_to_29_y_o_value = float(age[1].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    from_30_to_44_y_o = age[2].find('p').text.strip()
                    from_30_to_44_y_o_value = float(age[2].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    from_45_to_59_y_o = age[3].find('p').text.strip()
                    from_45_to_59_y_o_value = float(age[3].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    from_60_to_74_y_o = age[4].find('p').text.strip()
                    from_60_to_74_y_o_value = float(age[4].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    from_75_to_89_y_o = age[5].find('p').text.strip()
                    from_75_to_89_y_o_value = float(age[5].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    from_90_to_inf_y_o = age[6].find('p').text.strip()
                    from_90_to_inf_y_o_value = float(age[6].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    # Degree level
                    degree_level_title = population_graphs[1].find('h4').text.strip()
                    degree_level = population_graphs[1].find_all('div', {'class': 'bar'})
                    no_diploma = degree_level[0].find('p').text.strip()
                    no_diploma_value = float(degree_level[0].find('span', {'class': 'percentage'}).find('span').text.replace('%','').replace(',', '.').strip())
                    college_certificate = degree_level[1].find('p').text.strip()
                    college_certificate_value = float(degree_level[1].find('span', {'class': 'percentage'}).find('span').text.replace('%', '').replace(',','.').strip())
                    cap_bep = degree_level[2].find('p').text.strip()
                    cap_bep_value = float(degree_level[2].find('span', {'class': 'percentage'}).find('span').text.replace('%', '').replace(',','.').strip())
                    baccalaureate = degree_level[3].find('p').text.strip()
                    baccalaureate_value = float(degree_level[3].find('span', {'class': 'percentage'}).find('span').text.replace('%', '').replace(',','.').strip())
                    bac2_bac4 = degree_level[4].find('p').text.strip()
                    bac2_bac4_value = float(degree_level[4].find('span', {'class': 'percentage'}).find('span').text.replace('%', '').replace(',','.').strip())
                    bac3_bac4 = degree_level[5].find('p').text.strip()
                    bac3_bac4_value = float(degree_level[5].find('span', {'class': 'percentage'}).find('span').text.replace('%', '').replace(',','.').strip())
                    bac5_bac_inf = degree_level[6].find('p').text.strip()
                    bac5_bac_inf_value = float(degree_level[6].find('span', {'class': 'percentage'}).find('span').text.replace('%', '').replace(',','.').strip())
                # Climat
                climate = soup.find('section', {'id': 'climat'})
                climate_title = climate.find('h2').text.strip()
                climate_description = climate.find('div', {'class': 'dynamic-content'}).text.strip()
                climate_value = climate.find('ul', {'class': 'icon-cards'}).find_all('li')
                if len(climate_value) >= 6:
                    median_t = re.sub(r'\s+', '-', climate_value[0].find('p').text).strip()
                    max_t = re.sub(r'\s+', '-', climate_value[1].find('p').text).strip()
                    min_t = re.sub(r'\s+', '-', climate_value[2].find('p').text).strip()
                    median_warm = re.sub(r'\s+', '-', climate_value[3].find('p').text).strip()
                    median_rain = re.sub(r'\s+', '-', climate_value[4].find('p').text).strip()
                    median_cold = re.sub(r'\s+', '-', climate_value[5].find('p').text).strip()
                # Economy
                economy = soup.find('section', {'id': 'economie'})
                economy_title = economy.find('h2').text.strip() if economy else ''
                economy_elements = economy.find_all('div', {'class': 'card-content'}) if economy else []
                revenu_title = revenu_value = chomage_title = chomage_value = fibre_title = fibre_value = entreprises_title = entreprises_value = ''
                if len(economy_elements) >= 4:
                    revenu_title = economy_elements[0].find('p', {'class': 'p-content'}).text.strip()
                    revenu_value = economy_elements[0].find('div', {'class': 'text-content'}).find('p').text.strip()
                    chomage_title = economy_elements[1].find('p', {'class': 'p-content'}).text.strip()
                    chomage_value = economy_elements[1].find('div', {'class': 'text-content'}).find('p').text.strip()
                    fibre_title = economy_elements[2].find('p', {'class': 'p-content'}).text.strip()
                    fibre_value = economy_elements[2].find('div', {'class': 'text-content'}).find('p').text.strip()
                    entreprises_title = economy_elements[3].find('p', {'class': 'p-content'}).text.strip()
                    entreprises_value = economy_elements[3].find('div', {'class': 'text-content'}).find('p').text.strip()
                # Estate
                estate = soup.find('section',{'id':'immobilier'})
                estate_title = estate.find('h2').text.strip()
                estate_description = estate.find('div',{'class':'dynamic-content'}).text.strip()
                estate_price = estate.find_all('div',{'class':'card-content'})
                estate_info = estate.find_all('div',{'class':'graph-content'})
                # Price apartment
                median_price_m2_apartment = estate_price[0].find('div', {'class': 'text-content'}).find('p').text.replace('€', '').replace(',', '.').strip()
                median_price_m2_apartment_value = median_price_m2_apartment.replace(' ', '')
                price_apartment = re.findall(r'[-+]?\d[\d\s]*(?:\.\d+)?%?', median_price_m2_apartment_value)
                price_apartment_value = int(price_apartment[0].replace('\u202f', '').replace('-', '').replace('\n', '')) if price_apartment and price_apartment[0] and price_apartment[0] != '-' else None
                percentage_apartment_change = price_apartment[1] if len(price_apartment) > 1 else ""
                percentage_apartment_value = None
                if percentage_apartment_change:
                    percentage_apartment_parts = percentage_apartment_change.split('-', 1)
                    if percentage_apartment_parts and percentage_apartment_parts[0]:
                        percentage_apartment_value = float(percentage_apartment_parts[0].replace('%', '').replace('\u202f', '').replace('\n\n', ''))
                median_price_description_m2_apartment = estate_price[0].find('p',{'class':'p-content'}).text.strip()
                # Price mansion
                median_price_m2_mansion = estate_price[1].find('div', {'class': 'text-content'}).find('p').text.replace('€', '').replace(',', '.').strip()
                median_price_m2_mansion_value = median_price_m2_mansion.replace(' ', '')
                price_mansion = re.findall(r'[-+]?\d[\d\s]*(?:\.\d+)?%?', median_price_m2_mansion_value)
                price_mansion_value = int(price_mansion[0].replace('\u202f', '').replace('-', '').replace('\n\n','')) if price_mansion and price_mansion[0] and price_mansion[0] != '-' else None
                percentage_mansion_change = price_mansion[1] if len(price_mansion) > 1 else ""
                percentage_mansion_value = None
                if percentage_mansion_change:
                    percentage_mansion_parts = percentage_mansion_change.split('+', 1)
                    if percentage_mansion_parts and percentage_mansion_parts[0]:
                        percentage_mansion_value = float(
                            percentage_mansion_parts[0].replace('%', '').replace('\u202f', '').replace('\n\n', ''))
                median_price_description_m2_mansion = estate_price[1].find('p',{'class':'p-content'}).text.strip()
                # Graphs for Habitat types
                habitat_type = estate_info[0].find('div', {'class': 'pie-chart'})
                habitat_type_title = habitat_type.find('h4').text.strip()
                habitat_type1 = habitat_type.find('div', {'class': 'flex-wrapper'})
                habitat_type2 = habitat_type1.find('div', {'class': 'single-chart'})
                habitat_type3 = habitat_type2.find('svg', {'class': 'circular-chart'})
                # High value
                habitat_type_high_title = habitat_type3.find('text', {'class': 'name'}).text.strip()
                habitat_type_high = habitat_type3.find('text', {'class': 'text-one'})
                if habitat_type_high:
                    habitat_type_high_value = float(habitat_type_high.text.replace('%', '').replace(' ','').replace(',','.').strip())
                else:
                    habitat_type_high_value = None
                # Low value
                habitat_type_low = habitat_type.find('div', {'class': 'second-circle-content'}).find('p').text.replace(' ','').replace(',', '.').strip()
                parts = habitat_type_low.split('%')

                if habitat_type_low:
                    habitat_type_low_value = float(parts[0].strip())
                    habitat_type_low_title = parts[1].strip()
                else:
                    habitat_type_low_value = None
                    habitat_type_low_title = None

                print(habitat_type_low_value,habitat_type_low_title, city_code)


                city_data = {
                    'description': {
                        'city_code': city_code,
                        'name': name,
                        'presentation_title': presentation_title,
                        'presentation_body': presentation_body,
                        'population': {
                            'population_title':population_title,
                            'residents': residents,
                            'residents_value': residents_value,
                            'years_range': years_range,
                            'years_range_value': years_range_value,
                            'residents_km': residents_km,
                            'residents_km_value': residents_km_value,
                            'median_age': median_age,
                            'median_age_value': median_age_value,
                            'age_distribution': {
                                'age_distribution_title': age_distribution_title,
                                'from_0_to_14_y_o': from_0_to_14_y_o,
                                'from_0_to_14_y_o_value': from_0_to_14_y_o_value,
                                'from_15_to_29_y_o': from_15_to_29_y_o,
                                'from_15_to_29_y_o_value': from_15_to_29_y_o_value,
                                'from_30_to_44_y_o': from_30_to_44_y_o,
                                'from_30_to_44_y_o_value': from_30_to_44_y_o_value,
                                'from_45_to_59_y_o': from_45_to_59_y_o,
                                'from_45_to_59_y_o_value': from_45_to_59_y_o_value,
                                'from_60_to_74_y_o': from_60_to_74_y_o,
                                'from_60_to_74_y_o_value': from_60_to_74_y_o_value,
                                'from_75_to_89_y_o': from_75_to_89_y_o,
                                'from_75_to_89_y_o_value': from_75_to_89_y_o_value,
                                'from_90_to_inf_y_o': from_90_to_inf_y_o,
                                'from_90_to_inf_y_o_value': from_90_to_inf_y_o_value
                            },
                            'degree_level': {
                                'degree_level_title': degree_level_title,
                                'no_diploma': no_diploma,
                                'no_diploma_value': no_diploma_value,
                                'college_certificate': college_certificate,
                                'college_certificate_value': college_certificate_value,
                                'cap_bep': cap_bep,
                                'cap_bep_value': cap_bep_value,
                                'baccalaureate': baccalaureate,
                                'baccalaureate_value': baccalaureate_value,
                                'bac2_bac4': bac2_bac4,
                                'bac2_bac4_value': bac2_bac4_value,
                                'bac3_bac4': bac3_bac4,
                                'bac3_bac4_value': bac3_bac4_value,
                                'bac5_bac_inf': bac5_bac_inf,
                                'bac5_bac_inf_value': bac5_bac_inf_value
                            },
                        },
                        'climate': {
                            'climat_title': climate_title,
                            'climate_description': climate_description,
                            'median_t': median_t.replace('-', ' ').strip(),
                            'max_t': max_t.replace('-', ' ').strip(),
                            'min_t': min_t.replace('-', ' ').strip(),
                            'median_warm': median_warm.replace('-', ' ').strip(),
                            'median_rain': median_rain.replace('-', ' ').strip(),
                            'median_cold': median_cold.replace('-', ' ').strip(),
                        },
                        'economy': {
                            'economy_title': economy_title,
                            'revenu_title': revenu_title,
                            'revenu_value': revenu_value,
                            'chomage_title': chomage_title,
                            'chomage_value': chomage_value,
                            'fibre_title': fibre_title,
                            'fibre_value': fibre_value,
                            'entreprises_title': entreprises_title,
                            'entreprises_value': entreprises_value
                        },
                        'estate': {
                            'estate_title': estate_title,
                            'estate_description': estate_description,
                            'median_price_m2_apartment': price_apartment_value,
                            'median_percentage_m2_apartment': percentage_apartment_value,
                            'median_price_description_m2_apartment': median_price_description_m2_apartment,
                            'median_price_m2_mansion': price_mansion_value,
                            'median_percentage_m2_mansion': percentage_mansion_value,
                            'median_price_description_m2_mansion': median_price_description_m2_mansion,
                            'habitat_type_title': habitat_type_title,
                            'habitat_type_high_value': habitat_type_high_value,
                            'habitat_type_high_title': habitat_type_high_title,
                            'habitat_type_low_value': habitat_type_low_value,
                            'habitat_type_low_title': habitat_type_low_title,
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
                        'city_hall': {
                            'city_hall_name': None,
                            'city_hall_info': {
                                'address': None,
                                'phone': None,
                                'email': None,
                                'hours': None,
                            }
                        }
                    }
                }
                collection.update_one({'city_code': city_code}, {'$set': city_data}, upsert=True)
                total_requests += 1
                last_row_processed = row

            else:
                print(f"No data found for {city_code}")
        else:
            print(f"Failed to fetch data for {city_code}: {response.status_code}")
    return last_row_processed, total_requests

# DB Connect
client = MongoClient('mongodb+srv://admin:Agudib24@citydata.1uvq9yv.mongodb.net/')
db = client['Cities']
collection = db['test']

with open('cities_data_cleaned.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    total_requests = 0
    last_row_processed = None

    last_row_processed, total_requests = scrape_data(reader, collection, total_requests, last_row_processed)

    # Загружаем последний обработанный ряд (если он есть)
    try:
        with open('last_processed_row.txt', 'r', encoding='utf-8') as file:
            last_processed_row = file.readline().strip().split(',')
    except FileNotFoundError:
        last_processed_row = None

    # Сохраняем последний обработанный ряд
    if last_row_processed:
        with open('last_processed_row.txt', 'w', encoding='utf-8') as file:
            file.write(','.join(last_row_processed))

driver.quit()
