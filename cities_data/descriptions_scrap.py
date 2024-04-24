import time
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import csv
from pymongo import MongoClient
import re
import datetime
import traceback

start_time = time.time()

url = "https://www.villesavivre.fr"
# DB Connect
client = MongoClient('mongodb+srv://admin:Agudib24@citydata.1uvq9yv.mongodb.net/')
db = client['Cities']
collection = db['times']
try:
    while True:
        current_time = datetime.datetime.now().strftime("%H:%M:%S")
        print(f"\rTime: {current_time}", end="", flush=True)
        time.sleep(1)
        def scrape_data(reader, collection, total_requests, last_row_processed):
            for row in reader:
                if total_requests >= 500:
                    print(f"{total_requests} requests exceeded")
                    time.sleep(5)
                    total_requests = 0

                city_code = row[0]
                user_agent = UserAgent().random
                headers = {'User-Agent': user_agent}
                response = requests.get(f'{url}/{city_code}', headers=headers)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    city_element = soup.find('div', {'class': 'header-content'})

                    if city_element:
                        try:
                            # City Description
                            name = soup.find('div', {'class': 'header-content'}).find('h1').text.strip()
                            image_src = soup.find('div', {'class': 'header-cover'}).find('img')
                            image = image_src['src']
                            presentation = soup.find('section', {'id': 'presentation'})
                            presentation_title = presentation.find('h2').text.strip() if presentation else ''
                            presentation_body = soup.find('div', {'class': 'dynamic-content'}).text.strip() if soup.find('div', {'class': 'dynamic-content'}) else ''
                            # Population
                            population = soup.find('section', {'id': 'population'})
                            if population:
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
                            if climate:
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
                            if economy:
                                economy_title = economy.find('h2').text.strip() if economy else ''
                                economy_elements = economy.find_all('div', {'class': 'card-content'}) if economy else []
                                revenu_title = revenu_value = chomage_title = chomage_value = fibre_title = fibre_value = entreprises_title = entreprises_value = ''
                                if len(economy_elements) >= 3:
                                    revenu_title = economy_elements[0].find('p', {'class': 'p-content'}).text.strip()
                                    revenu_value = economy_elements[0].find('div', {'class': 'text-content'}).find('p').text.strip() if economy_elements and economy_elements[0] and economy_elements[0] != '-' else None
                                    chomage_title = economy_elements[1].find('p', {'class': 'p-content'}).text.strip()
                                    chomage_value = economy_elements[1].find('div', {'class': 'text-content'}).find('p').text.strip()
                                    fibre_title = economy_elements[2].find('p', {'class': 'p-content'}).text.strip()
                                    fibre_value = economy_elements[2].find('div', {'class': 'text-content'}).find('p').text.strip()
                                    entreprises_title = economy_elements[3].find('p', {'class': 'p-content'}).text.strip()
                                    entreprises_value = economy_elements[3].find('div', {'class': 'text-content'}).find('p').text.strip() if economy_elements and economy_elements[3] and economy_elements[3] != '-' else None
                            # Estate
                            estate = soup.find('section', {'id':'immobilier'})
                            if estate:
                                estate_title = estate.find('h2').text.strip()
                                estate_description = estate.find('div', {'class':'dynamic-content'}).text.strip()
                                estate_price = estate.find_all('div', {'class': 'card-content'})
                                estate_info = estate.find_all('div', {'class': 'graph-content'})
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
                                # Estate use
                                estate_use = estate_info[1].find('div', {'class': 'pie-chart'})
                                estate_use_elements = estate_info[1].find_all('div', {'class': 'bar'})
                                estate_use_title = estate_use.find('h4').text.strip()
                                if len(estate_use_elements) >= 2:
                                    vacant_housing_value = float(estate_use_elements[0].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                    vacant_housing_title = estate_use_elements[0].find('p').text.strip()
                                    main_residences_title = estate_use_elements[1].find('p').text.strip()
                                    main_residences_value = float(estate_use_elements[1].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                    secondary_residences_title = estate_use_elements[2].find('p').text.strip()
                                    secondary_residences_value = float(estate_use_elements[2].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                # Estate Rooms
                                number_of_rooms = estate_info[3].find('div', {'class': 'pie-chart'})
                                number_of_rooms_elements = estate_info[3].find_all('div', {'class': 'bar'})
                                number_of_rooms_title = number_of_rooms.find('h4').text.strip()
                                if len(number_of_rooms_elements) >= 4:
                                    one_room_title = number_of_rooms_elements[0].find('p').text.strip()
                                    one_room_value = float(number_of_rooms_elements[0].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                    two_rooms_title = number_of_rooms_elements[1].find('p').text.strip()
                                    two_rooms_value = float(number_of_rooms_elements[1].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                    three_rooms_title = number_of_rooms_elements[2].find('p').text.strip()
                                    three_rooms_value = float(number_of_rooms_elements[2].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                    four_rooms_title = number_of_rooms_elements[3].find('p').text.strip()
                                    four_rooms_value = float(number_of_rooms_elements[3].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                    five_rooms_title = number_of_rooms_elements[4].find('p').text.strip()
                                    five_rooms_value = float(number_of_rooms_elements[4].find('span').text.replace(' ', '').replace('%', '').replace(',', '.').strip())
                                # Residents
                                rsidents_elements = estate_info[2].find('div', {'class': 'pie-chart'})
                                rsidents_title = rsidents_elements.find('h4').text.strip()
                                # Graphs for residents
                                rsidents_elements_type1 = rsidents_elements.find('div', {'class': 'flex-wrapper'})
                                rsidents_elements_type2 = rsidents_elements_type1.find('div', {'class': 'single-chart'})
                                rsidents_elements_type3 = rsidents_elements_type2.find('svg', {'class': 'circular-chart'})
                                # High value
                                residents_high_title = rsidents_elements_type3.find('text', {'class': 'name'}).text.strip()
                                residents_high = rsidents_elements_type3.find('text', {'class': 'text-one'})
                                if residents_high:
                                    residents_high_value = float(
                                        residents_high.text.replace('%', '').replace(' ', '').replace(',', '.').strip())
                                else:
                                    residents_high_value = None
                                # Low value
                                residents_low = rsidents_elements.find('div', {'class': 'second-circle-content'}).find('p').text.replace(
                                    ' ', '').replace(',', '.').strip()
                                parts = residents_low.split('%')

                                if residents_low:
                                    residents_low_value = float(parts[0].strip())
                                    residents_low_title = parts[1].strip()
                                else:
                                    residents_low_value = None
                                    residents_low_title = None
                            # Security
                            security = soup.find('section', {'id': 'securite'})
                            if security:
                                security_title = security.find('h2').text.strip()
                                security_value = int(security.find('div', {'class': 'security-div'}).find('div', {'class': 'security-content'}).find('div', {'class': 'city-crime'}).find('p', {'class': 'security-number'}).text.replace('\u202f', '').strip())
                                security_value_element = security.find('div', {'class': 'security-div'}).find('div', {'class': 'security-content'}).find('div', {'class': 'city-crime'}).find_all('p')
                                security_value_title = security_value_element[1].text.strip()
                                median_national_element = security.find('div', {'class': 'security-div'}).find('div', {'class': 'security-content'}).find_all('p')
                                median_national_value = median_national_element[2].text.strip()
                                security_description = median_national_element[3].text.replace('/n',' ').strip()
                                security_description_element = ' '.join(security_description.split())
                                crimes_table = security.find('div', {'class': 'security-div'}).find('table').find('tbody').find_all('tr')
                                if len(crimes_table) >= 4:
                                    burglaries = crimes_table[0].find_all('td')
                                    burglaries_title = crimes_table[0].find('th').text.strip()
                                    burglaries_national = int(burglaries[1].text.replace('\u202f', '').strip())
                                    burglaries_local = int(burglaries[0].text.replace('\u202f', '').strip())
                                    auto_thefts = crimes_table[1].find_all('td')
                                    auto_thefts_title = crimes_table[1].find('th').text.strip()
                                    auto_thefts_national = int(auto_thefts[1].text.replace('\u202f', '').strip())
                                    auto_thefts_local = int(auto_thefts[0].text.replace('\u202f', '').strip())
                                    private_thefts = crimes_table[2].find_all('td')
                                    private_thefts_title = crimes_table[2].find('th').text.strip()
                                    private_thefts_national = int(private_thefts[1].text.replace('\u202f', '').strip())
                                    private_thefts_local = int(private_thefts[0].text.replace('\u202f', '').strip())
                                    physical_violence = crimes_table[3].find_all('td')
                                    physical_violence_title = crimes_table[3].find('th').text.strip()
                                    physical_violence_national = int(physical_violence[1].text.replace('\u202f', '').strip())
                                    physical_violence_local = int(physical_violence[0].text.replace('\u202f', '').strip())
                                    sex_violence = crimes_table[4].find_all('td')
                                    sex_violence_title = crimes_table[4].find('th').text.strip()
                                    sex_violence_national = int(sex_violence[1].text.replace('\u202f', '').strip())
                                    sex_violence_local = int(sex_violence[0].text.replace('\u202f', '').strip())
                            # Policy
                            policy = soup.find('section', {'id': 'politique'})
                            if policy:
                                policy_elements = policy.find('div', {'class': 'graph-content'}).find_all('div', {'class': 'flex-wrapper'})
                                policy_title = policy.find('h2').text.strip()
                                policy_description = policy.find('p').text.strip()
                                # Year one
                                year_1_elements = policy_elements[0].find('div', {'class': 'single-chart'}).find_all('div')
                                year_1 = int(policy_elements[0].find('div', {'class': 'single-chart'}).find('div', {'class': 'year'}).text.strip())
                                winner_value = float(policy_elements[0].find('div', {'class': 'single-chart'}).find('svg', {'class': 'circular-chart'}).find('text').text.replace('%', '').replace(',','.').strip())
                                candidate_winner = year_1_elements[2].find('p').text.strip()
                                candidate_looser_inf = year_1_elements[4].find('p').text.replace(',', '.').strip()
                                candidate_parts = candidate_looser_inf.split('%')
                                if candidate_looser_inf:
                                    looser_value = float(candidate_parts[0].strip())
                                    candidate_looser = candidate_parts[1].strip()
                                else:
                                    candidate_looser = None
                                    looser_value = None
                                # Year two
                                year_2_elements = policy_elements[1].find('div', {'class': 'single-chart'}).find_all('div')
                                year_2 = int(policy_elements[1].find('div', {'class': 'single-chart'}).find('div', {
                                    'class': 'year'}).text.strip())
                                winner_value_2 = float(policy_elements[1].find('div', {'class': 'single-chart'}).find('svg', {
                                    'class': 'circular-chart'}).find('text').text.replace('%', '').replace(',', '.').strip())
                                candidate_winner_2 = year_2_elements[2].find('p').text.strip()
                                candidate_looser_inf_2 = year_2_elements[4].find('p').text.replace(',', '.').strip()
                                candidate_parts_2 = candidate_looser_inf_2.split('%')
                                if candidate_looser_inf_2:
                                    looser_value_2 = float(candidate_parts_2[0].strip())
                                    candidate_looser_2 = candidate_parts_2[1].strip()
                                else:
                                    looser_value_2 = None
                                    candidate_looser_2 = None
                                # Year three
                                year_3_elements = policy_elements[2].find('div', {'class': 'single-chart'}).find_all('div')
                                year_3 = int(policy_elements[2].find('div', {'class': 'single-chart'}).find('div', {
                                    'class': 'year'}).text.strip())
                                winner_value_3 = float(policy_elements[2].find('div', {'class': 'single-chart'}).find('svg', {
                                    'class': 'circular-chart'}).find('text').text.replace('%', '').replace(',', '.').strip())
                                candidate_winner_3 = year_3_elements[2].find('p').text.strip()
                                candidate_looser_inf_3 = year_3_elements[4].find('p').text.replace(',', '.').strip()
                                candidate_parts_3 = candidate_looser_inf_3.split('%')
                                if candidate_looser_inf_3:
                                    looser_value_3 = float(candidate_parts_3[0].strip())
                                    candidate_looser_3 = candidate_parts_3[1].strip()
                                else:
                                    looser_value_3 = None
                                    candidate_looser_3 = None
                                # Year four
                                year_4_elements = policy_elements[3].find('div', {'class': 'single-chart'}).find_all('div')
                                year_4 = int(policy_elements[3].find('div', {'class': 'single-chart'}).find('div', {
                                    'class': 'year'}).text.strip())
                                winner_value_4 = float(policy_elements[3].find('div', {'class': 'single-chart'}).find('svg', {
                                    'class': 'circular-chart'}).find('text').text.replace('%', '').replace(',', '.').strip())
                                candidate_winner_4 = year_4_elements[2].find('p').text.strip()
                                candidate_looser_inf_4 = year_4_elements[4].find('p').text.replace(',', '.').strip()
                                candidate_parts_4 = candidate_looser_inf_4.split('%')
                                if candidate_looser_inf_4:
                                    looser_value_4 = float(candidate_parts_4[0].strip())
                                    candidate_looser_4 = candidate_parts_4[1].strip()
                                else:
                                    looser_value_4 = None
                                    candidate_looser_4 = None
                            # Services
                            service = soup.find('section', {'id': 'services'})
                            if service:
                                services_title = service.find('h2').text.strip()
                                services = service.find('div', {'class': 'tabs'}).find_all('div', {'class': 'tab-div'})
                                # Health
                                health = services[0].find('table')
                                health_element = health.find('thead').find('tr').find_all('th')
                                health_title = health_element[1].text.strip()
                                health_services = health.find('tbody').find_all('tr')
                                doctor = health_services[0].find('th').text.strip()
                                doctor_value = health_services[0].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                dentist = health_services[1].find('th').text.strip()
                                dentist_value = health_services[1].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                nurse = health_services[2].find('th').text.strip()
                                nurse_value = health_services[2].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                pharmacy = health_services[3].find('th').text.strip()
                                pharmacy_value = health_services[3].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                emergencies = health_services[4].find('th').text.strip()
                                emergencies_value = health_services[4].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                # Education
                                education = services[1].find('table')
                                education_element = education.find('thead').find('tr').find_all('th')
                                education_title = education_element[1].text.strip()
                                education_services = education.find('tbody').find_all('tr')
                                nursery = education_services[0].find('th').text.strip()
                                nursery_value = education_services[0].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                kindergarten = education_services[1].find('th').text.strip()
                                kindergarten_value = education_services[1].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                elementary_school = education_services[2].find('th').text.strip()
                                elementary_school_value = education_services[2].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                college = education_services[3].find('th').text.strip()
                                college_value = education_services[3].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                high_school = education_services[4].find('th').text.strip()
                                high_school_value = education_services[4].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                # Shops
                                shops = services[2].find('table')
                                shops_element = shops.find('thead').find('tr').find_all('th')
                                shops_title = shops_element[1].text.strip()
                                shops_services = shops.find('tbody').find_all('tr')
                                hypermarket = shops_services[0].find('th').text.strip()
                                hypermarket_value = shops_services[0].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                supermarket = shops_services[1].find('th').text.strip()
                                supermarket_value = shops_services[1].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                grocery_store = shops_services[2].find('th').text.strip()
                                grocery_store_value = shops_services[2].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                bakery = shops_services[3].find('th').text.strip()
                                bakery_value = shops_services[3].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                gas_station = shops_services[4].find('th').text.strip()
                                gas_station_value = shops_services[4].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                post_office = shops_services[5].find('th').text.strip()
                                post_office_value = shops_services[5].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                # Transport & hobbies
                                transport_hobbies = services[3].find('table')
                                transport_hobbies_element = transport_hobbies.find('thead').find('tr').find_all('th')
                                transport_hobbies_title = transport_hobbies_element[1].text.strip()
                                transport_hobbies_services = transport_hobbies.find('tbody').find_all('tr')
                                hotel = transport_hobbies_services[0].find('th').text.strip()
                                hotel_value = transport_hobbies_services[0].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                restaurant = transport_hobbies_services[1].find('th').text.strip()
                                restaurant_value = transport_hobbies_services[1].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                cinema = transport_hobbies_services[2].find('th').text.strip()
                                cinema_value = transport_hobbies_services[2].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                library = transport_hobbies_services[3].find('th').text.strip()
                                library_value = transport_hobbies_services[3].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                SNCF_train_station = transport_hobbies_services[4].find('th').text.strip()
                                SNCF_train_station_value = transport_hobbies_services[4].find('td').text.replace('\n ', '').replace('  ', '').strip()
                                airport = transport_hobbies_services[5].find('th').text.strip()
                                airport_value = transport_hobbies_services[5].find('td').text.replace('\n ', '').replace('  ', '').strip()
                            # City hall
                            city_hall = soup.find('section', {'id': 'cityhall'})
                            if city_hall:
                                city_hall_elements = city_hall.find_all('p')
                                print(city_code)
                                if len(city_hall_elements) >= 5:
                                    city_hall_name = city_hall.find('h2').text.strip()
                                    city_hall_descriptions = city_hall_elements[0].text.replace('\n ', '').replace('  ', '').strip()
                                    address = city_hall_elements[1].text.strip()
                                    if address:
                                        address_parts = address.split(':')
                                        if len(address_parts) == 2:
                                            address_title = address_parts[0]
                                            address_value = address_parts[1]
                                        else:
                                            address_title = None
                                            address_value = None
                                    else:
                                        address_title = None
                                        address_value = None
                                    phone = city_hall_elements[2].text.replace('  ', '').strip()
                                    if phone:
                                        phone_parts = phone.split(':')
                                        if len(phone_parts) == 2:
                                            phone_title = phone_parts[0]
                                            phone_value = phone_parts[1].replace('\n ', '').replace(' ', '.')
                                        else:
                                            phone_title = None
                                            phone_value = None
                                    else:
                                        phone_title = None
                                        phone_value = None
                                    email = city_hall_elements[3].text.replace('  ', '').strip()
                                    if email:
                                        email_parts = email.split(':')
                                        if len(email_parts) == 2:
                                            email_title = email_parts[0]
                                            email_value = email_parts[1].replace('\n ', '').replace(' ', '.')
                                        else:
                                            email_title = None
                                            email_value = None
                                    else:
                                        email_title = None
                                        email_value = None
                                    hours = city_hall_elements[4]
                                    hours_element = city_hall.find('ul').find('li')
                                    if hours:
                                        hours_title = city_hall_elements[4].text.strip()
                                    else:
                                        hours_title = None
                                    if hours_element:
                                        hours_value = hours_element.text.replace('\n ', '').replace('  ', '').strip()
                                    else:
                                        hours_value = None
                                else:
                                    city_hall_name = city_hall.find('h2').text.strip()
                                    city_hall_descriptions = None
                                    address = city_hall_elements[0].text.strip()
                                    address_parts = address.split(':')
                                    if address:
                                        address_title = address_parts[0]
                                        address_value = address_parts[1]
                                    else:
                                        address_title = None
                                        address_value = None
                                    phone = city_hall_elements[1].text.replace('  ', '').strip()
                                    phone_parts = phone.split(':')
                                    if phone:
                                        phone_title = phone_parts[0]
                                        phone_value = phone_parts[1].replace('\n ', '').replace(' ', '.')
                                    else:
                                        phone_title = None
                                        phone_value = None
                                    email = city_hall_elements[2].text.replace('  ', '').strip()
                                    email_parts = email.split(':')
                                    if email:
                                        email_title = email_parts[0]
                                        email_value = email_parts[1].replace('\n ', '').replace(' ', '.')
                                    else:
                                        email_title = None
                                        email_value = None
                                    print(city_code)
                                    hours = city_hall_elements[3]
                                    hours_element = city_hall.find('ul').find('li')
                                    if hours:
                                        hours_title = city_hall_elements[3].text.strip()
                                    else:
                                        hours_title = None
                                    if hours_element:
                                        hours_value = hours_element.text.replace('\n ', '').replace('  ', '').strip()
                                    else:
                                        hours_value = None
                            else:
                               city_hall_name = None,
                               city_hall_descriptions = None,
                               address_title = None,
                               address_value = None,
                               phone_title = None,
                               phone_value = None,
                               email_title = None,
                               email_value = None,
                               hours_title = None,
                               hours_value = None
                        except IndexError as e:
                            print(f"Произошла ошибка в списке: {e}")
                            with open('loss.csv', 'a', newline='', encoding='utf-8') as csvfile:
                                writer = csv.writer(csvfile)
                                writer.writerow([city_code])

                        city_data = {
                            'description': {
                                'city_code': city_code,
                                'name': name,
                                'image': image,
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
                                    'entreprises_value': entreprises_value,
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
                                        'estate_use_title': estate_use_title,
                                        'vacant_housing_title': vacant_housing_title,
                                        'vacant_housing_value': vacant_housing_value,
                                        'main_residences_title': main_residences_title,
                                        'main_residences_value': main_residences_value,
                                        'secondary_residences_title': secondary_residences_title,
                                        'secondary_residences_value': secondary_residences_value
                                    },
                                    'number_of_rooms': {
                                        'number_of_rooms_title': number_of_rooms_title,
                                        'one_room_title': one_room_title,
                                        'one_room_value': one_room_value,
                                        'two_rooms_title': two_rooms_title,
                                        'two_rooms_value': two_rooms_value,
                                        'three_rooms_title': three_rooms_title,
                                        'three_rooms_value': three_rooms_value,
                                        'four_rooms_title': four_rooms_title,
                                        'four_rooms_value': four_rooms_value,
                                        'five_rooms_title': five_rooms_title,
                                        'five_rooms_value': five_rooms_value
                                    },
                                    'residents': {
                                        'residents_title': rsidents_title,
                                        'residents_high_value': residents_high_value,
                                        'residents_high_title': residents_high_title,
                                        'residents_low_value': residents_low_value,
                                        'residents_low_title': residents_low_title,
                                    },

                                },
                                'security': {
                                    'security_title': security_title,
                                    'security_value': security_value,
                                    'security_value_title': security_value_title,
                                    'median_national_value': median_national_value,
                                    'security_description': security_description_element,
                                    'crimes_type': {
                                        'burglaries_title': burglaries_title,
                                        'auto_thefts_title': auto_thefts_title,
                                        'private_thefts_title': private_thefts_title,
                                        'physical_violence_title': physical_violence_title,
                                        'sex_violence_title': sex_violence_title
                                    },
                                    'crimes_national': {
                                        'burglaries_national': burglaries_national,
                                        'auto_thefts_national': auto_thefts_national,
                                        'private_thefts_national': private_thefts_national,
                                        'physical_violence_national': physical_violence_national,
                                        'sex_violence_national': sex_violence_national
                                    },
                                    'crimes_local': {
                                        'burglaries_local': burglaries_local,
                                        'auto_thefts_local': auto_thefts_local,
                                        'private_thefts_local': private_thefts_local,
                                        'physical_violence_local': physical_violence_local,
                                        'sex_violence_local': sex_violence_local
                                    },
                                },
                                'policy': {
                                    'policy_title': policy_title,
                                    'policy_description': policy_description,
                                    'year_1': {
                                        'year_1': year_1,
                                        'candidate_winner': candidate_winner,
                                        'winner_value': winner_value,
                                        'candidate_looser': candidate_looser,
                                        'looser_value': looser_value
                                    },
                                    'year_2': {
                                        'year_2': year_2,
                                        'candidate_winner_2': candidate_winner_2,
                                        'winner_value_2': winner_value_2,
                                        'candidate_looser_2': candidate_looser_2,
                                        'looser_value_2': looser_value_2
                                    },
                                    'year_3': {
                                        'year_3': year_3,
                                        'candidate_winner_3': candidate_winner_3,
                                        'winner_value_3': winner_value_3,
                                        'candidate_looser_3': candidate_looser_3,
                                        'looser_value_3': looser_value_3
                                    },
                                    'year_4': {
                                        'year_4': year_4,
                                        'candidate_winner_4': candidate_winner_4,
                                        'winner_value_4': winner_value_4,
                                        'candidate_looser_4': candidate_looser_4,
                                        'looser_value_4': looser_value_4
                                    },
                                },
                                'services': {
                                    'services_title': services_title,
                                    'services_types': {
                                        'health': {
                                            'health_title': health_title,
                                            'doctor': doctor,
                                            'doctor_value': doctor_value,
                                            'dentist': dentist,
                                            'dentist_value': dentist_value,
                                            'nurse': nurse,
                                            'nurse_value': nurse_value,
                                            'pharmacy': pharmacy,
                                            'pharmacy_value': pharmacy_value,
                                            'emergencies': emergencies,
                                            'emergencies_value': emergencies_value
                                        },
                                        'education': {
                                            'education_title': education_title,
                                            'nursery': nursery,
                                            'nursery_value': nursery_value,
                                            'kindergarten': kindergarten,
                                            'kindergarten_value': kindergarten_value,
                                            'elementary_school': elementary_school,
                                            'elementary_school_value': elementary_school_value,
                                            'college': college,
                                            'college_value': college_value,
                                            'high_school': high_school,
                                            'high_school_value': high_school_value
                                        },
                                        'shops': {
                                            'shops_title': shops_title,
                                            'hypermarket': hypermarket,
                                            'hypermarket_value': hypermarket_value,
                                            'supermarket': supermarket,
                                            'supermarket_value': supermarket_value,
                                            'grocery_store': grocery_store,
                                            'grocery_store_value': grocery_store_value,
                                            'bakery': bakery,
                                            'bakery_value': bakery_value,
                                            'gas_station': gas_station,
                                            'gas_station_value': gas_station_value,
                                            'post_office': post_office,
                                            'post_office_value': post_office_value

                                        },
                                        'transport_hobbies': {
                                            'transport_hobbies_title': transport_hobbies_title,
                                            'hotel': hotel,
                                            'hotel_value': hotel_value,
                                            'restaurant': restaurant,
                                            'restaurant_value': restaurant_value,
                                            'cinema': cinema,
                                            'cinema_value': cinema_value,
                                            'library': library,
                                            'library_value': library_value,
                                            'SNCF_train_station': SNCF_train_station,
                                            'SNCF_train_station_value': SNCF_train_station_value,
                                            'airport': airport,
                                            'airport_value': airport_value
                                        },
                                    }
                                },
                                'city_hall': {
                                    'city_hall_name': city_hall_name,
                                    'city_hall_info': {
                                        'city_hall_descriptions': city_hall_descriptions,
                                        'address_title': address_title,
                                        'address_value': address_value,
                                        'phone_title': phone_title,
                                        'phone_value': phone_value,
                                        'email_title': email_title,
                                        'email_value': email_value,
                                        'hours_title': hours_title,
                                        'hours_value': hours_value
                                    }
                                }
                            }
                        }
                        collection.update_one({'city_code': city_code}, {'$set': city_data}, upsert=True)
                        total_requests += 1

                        last_row_processed = row

                    else:
                        print(f"No data found for {city_code}")
                        print(f"Request Count: {total_requests}")
                else:
                    print(f"Failed to fetch data for {city_code}: {response.status_code}")
                    with open('loss.csv', 'a', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([city_code])
            return last_row_processed, total_requests


        with open('cities_data_cleaned.csv', 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            for _ in range(16649):
                next(reader)
            total_requests = 0
            last_row_processed = None

            last_row_processed, total_requests = scrape_data(reader, collection, total_requests, last_row_processed)

            try:
                with open('last_processed_row.txt', 'r', encoding='utf-8') as file:
                    last_processed_row = file.readline().strip().split(',')
            except FileNotFoundError:
                last_processed_row = None

            if last_row_processed:
                with open('last_processed_row.txt', 'w', encoding='utf-8') as file:
                    file.write(','.join(last_row_processed))
except KeyboardInterrupt:
    print("\nStoped by User")
except Exception as e:
    print(f"\nПроизошла ошибка: {e}")
    traceback.print_exc()
    print(f"Request Count: {total_requests}")

elapsed_time = time.time() - start_time
print(f"Time: {elapsed_time:.2f} seconds")
