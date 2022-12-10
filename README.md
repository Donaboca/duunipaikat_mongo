# duunipaikat_mongo

Projektin tarkoitus on toimia työnäytteenä ja todentaa seuraavilla osa-alueilla ainakin perusteiden hallintaa:
- Python
- Pymongo ja MongoDB
- Verkkoharavointi / web scraping: Python, Requests, BeautifulSoup, Selenium
- Pandas
- Jupyter Notebook
- Dokumentointi

Lyhyt kuvaus projektista:
- Haetaan max viikon vanhat työpaikkailmoitukset palveluiden MOL, Monster ja Oikotie verkkosivuilta. Työpaikkailmoitukset tallennetaan MongoDB-tietokantaan.
- Tietokannasta voidaan ottaa eri listauksia ruudulle, esim. tänään ilmoitetut/tallennetut työpaikat, x tuoreinta paikkaa per palvelu tai paikat, joiden otsikossa esiintyy sana 'data'.

Vaatimukset:
- Jupyter Notebook
- Tarvittavat kirjastot: requests, bs4, selenium, fake_useragent, pymongo, pandas, colorama
- MongoDB (käytetty versio 4.2)

Puutteet / kehityskohteet:
- Mahdollinen virtuaaliympäristö tai Docker
- requests.txt tiedosto
- Itse koodissa on myös kehityskohteita, mutta osa johtuu verkkopalveluiden (MOL, Monster ja Oikotie) poikkeavista toteutustavoista (tämä vain huomiona, ei selityksenä).

Loppusanat:
- Projektin tarkoitus oli pyrkiä näyttämään osaamista yllä luetelluista osa-alueista. Aikarajoitteen vuoksi tavoitteena ei ollut täydellinen lopputulos, mutta ei myöskään ratkaisu missä aita on matalin.
