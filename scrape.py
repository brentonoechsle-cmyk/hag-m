import requests
from bs4 import BeautifulSoup

base_url = "https://www.rottentomatoes.com/top/bestofrt/?page="
page_num = 1

while True:
   url = base_url + str(page_num)
   response = requests.get(url)
   soup = BeautifulSoup(response.content, "html.parser")

   movies = soup.select("table.table tr")
   if not movies:
       break

   for movie in movies:
       title = movie.select_one(".unstyled.articleLink")
       score = movie.select_one(".tMeterScore")
       # Extract and process movie data

   page_num += 1