from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import csv

options = Options()
options.add_argument('headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)

details = []

with open('cities_data_cleaned.csv', 'r', encoding='utf-8') as csvfile, \
     open('economy.csv', 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.reader(csvfile)
    next(reader)
    writer = csv.writer(outfile)
    writer.writerow(['economy_title', 'revenu_title', 'revenu_value', 'chomage_title', 'chomage_value', 'fibre_title', 'fibre_value', 'entreprises_title', 'entreprises_value'])

    for row in reader:
        city_code = row[0]
        driver.implicitly_wait(15)
        driver.get(f'https://www.villesavivre.fr/{city_code}')
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        city_element = soup.find('div', {'class': 'header-content'})

        if city_element:
            population = soup.find('section', {'id': 'economie'})
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

            economy = {
                'economy_title': economy_title,
                'revenu_title': revenu_title,
                'revenu_value': revenu_value,
                'chomage_title': chomage_title,
                'chomage_value': chomage_value,
                'fibre_title': fibre_title,
                'fibre_value': fibre_value,
                'entreprises_title': entreprises_title,
                'entreprises_value': entreprises_value
            }
            details.append(economy)
            writer.writerow([city_code, economy_title, revenu_title, revenu_value, chomage_title, chomage_value, fibre_title, fibre_value, entreprises_title, entreprises_value])

        else:
            print(f"No data found for {city_code}")

driver.quit()