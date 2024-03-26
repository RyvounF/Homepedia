import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
import csv

options = Options()
options.add_argument('headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)
url = "https://www.villesavivre.fr"


def scrape_data(reader, writer, total_requests, last_row_processed):
    for row in reader:
        if total_requests >= 500:
            print("Достигнуто 500 запросов. Ожидание...")
            time.sleep(60)
            total_requests = 0

        city_code = row[0]

        # Генерируем случайный User-Agent
        user_agent = UserAgent().random
        headers = {'User-Agent': user_agent}

        # Делаем запрос с использованием сгенерированного User-Agent
        response = requests.get(f'https://www.villesavivre.fr/{city_code}', headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            city_element = soup.find('div', {'class': 'header-content'})

            if city_element:
                name = soup.find('div', {'class': 'header-content'}).find('h1').text.strip()
                presentation_section = soup.find('section', {'class': 'city-content'})
                presentation_title = presentation_section.find('h2').text.strip() if presentation_section else ''
                presentation_body = soup.find('div', {'class': 'dynamic-content'}).text.strip() if soup.find('div', {
                    'class': 'dynamic-content'}) else ''

                writer.writerow([city_code, name, presentation_title, presentation_body])

                total_requests += 1
                last_row_processed = row

            else:
                print(f"No data found for {city_code}")
        else:
            print(f"Failed to fetch data for {city_code}: {response.status_code}")
    return last_row_processed, total_requests


options = Options()
options.add_argument('headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)
url = "https://www.villesavivre.fr"

with open('cities_data_cleaned1.csv', 'r', encoding='utf-8') as csvfile, \
        open('cities_description1.csv', 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(csvfile)
    next(reader)
    writer = csv.writer(outfile)
    writer.writerow(['city_code', 'name', 'presentation_title', 'presentation_body'])
    total_requests = 0
    last_row_processed = None

    last_row_processed, total_requests = scrape_data(reader, writer, total_requests, last_row_processed)

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