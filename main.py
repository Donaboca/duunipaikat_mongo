import requests
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
from pymongo import MongoClient
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime as dt
from datetime import timedelta as td
# from datetime import date
import time
import os
from colorama import Back, Style
from IPython.display import clear_output
import time
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException
from multiprocessing import Process
import logging

# Mongo-client, localhost
client = MongoClient()

# Valitaan ja tarvittaessa luodaan tietokanta
db = client.duunit

# Valitaan / luodaan kokoelma
duunit = db.duunit

# Hakua varten, max. viikon vanhat ilmoitukset
tanaan = dt.today()
historia_pvm = tanaan - td(7)
historia_pvm = historia_pvm.replace(hour=0, minute=0, second=0, microsecond=0)

# Lokitiedosto
lokitiedosto = "Paikat_projektin_loki.log"


def kirjoita_tietokantaan(nimike, tyopaikka, kunta, pvm, linkki, lahde, duunit):
    """
    Funtio tarkistaa löytyykö tietokannasta dokumenttia käyttäen neljää eri kentän arvoa:
    - nimike
    - tyopaikka
    - kunta
    - lähde

    Dokumentti kirjoitetaan tai päivitetään tietokantaan. Päivityksessä otetaan huomioon ilmoituksen päivämäärä:
    Vain tuoreempi päivämäärä tuodaan kantaan päivityksen yhteydessä. Vanhempi pvm ei siis voi korvata tuoreempaa
    pvm:ää.

    Parametrit
    ----------
    nimike: str
    tyopaikka: str
    kunta: str
    pvm: datetime
    linkki: str
    lahde: str, arvo jokin seuraavista: Mol, Monster tai Oikotie.

    Palauttaa
    ---------
    Funktio ei palauta mitään.
    """

    tulos = duunit.update_one({
        "nimike": nimike,
        "tyopaikka": tyopaikka,
        "kunta": kunta,
        "lähde": lahde
    },
        [{
            "$set": {
                "nimike": nimike,
                "tyopaikka": tyopaikka,
                "kunta": kunta,
                "url": linkki,
                "ilmoitettu": {
                    "$cond": [{"$lt": ["$ilmoitettu", pvm]}, pvm, "$ilmoitettu"]},
                "lähde": lahde
            }}],
        upsert=True
    )
    return tulos


def monster_sivulta_tietokannassa_lkm(paikat, tyopaikat, kunnat, lahde):
    """
    Funtio tarkistaa löytyykö tietokannasta Monsterin 20 tuoreinta työpaikkailmoitusta (= kaikki 1. sivun ilmoitukset).

    Parametrit
    ----------
    paikat: lista, jonka alkiot ovat bs4.element.NavigableString tyyppiä.
    tyopaikat: lista, jonka alkiot str-tyyppiä.
    kunnat: lista, jonka alkiot bs4.element.Tag tyyppiä.
    lahde: str, arvo jokin seuraavista: Mol, Monster tai Oikotie.

    Palauttaa
    ---------
    lkm: int, löytyneiden työpaikkojen määrä tietokannasta, 0-20.
    """
    lkm = 0
    for paikka in paikat:
        if paikka.string is not None:
            nimike = paikka.string
            nimike = nimike.replace("&auml;", "ä").strip()
            tyopaikka = tyopaikat[lkm]
            kunta = kunnat[lkm].text
            kunta = kunta.replace("\n", "").strip()
            if duunit.find_one({"nimike": nimike, "tyopaikka": tyopaikka, "kunta": kunta, "lähde": lahde}):
                lkm += 1

    return lkm


def monster_seuraava_sivu(ilmoitettu):
    """
    Tarkistaa onko Monsterista haetun sivun vanhin ilmoitus uudempi kuin määritelty hakuaika
    (muuttujan historia_pvm arvo).

    Parametrit
    ----------
    ilmoitettu: lista, jonka sisältö bs4.element.ResultSet muodossa ja jossa on Monsterissa olevien ilmoitusten pvm:t.

    Palauttaa
    ---------
    True: boolean, jos haetun sivun vanhin ilmoitus on tuoreempi/yhtä tuore kuin määritelty aikaraja
    (muuttuja historia_pvm).
    False: boolean, jos ilmoitus on vanhempi kuin määritelty aikaraja.
    """
    ilmoitus_paivat = [dt.strptime(pvm.text.replace(",", "").strip(), "%d.%m.%Y") for pvm in ilmoitettu]

    # vanhin_ilmoitus = min(ilmoitus_paivat)
    if min(ilmoitus_paivat) >= historia_pvm:
        return True
    else:
        return False


def hae_suomen_kunnat():
    """
    Hakee Wikipediasta listan Suomen kunnista.

    Parametrit
    ----------
    Ei parametreja.

    Palauttaa
    ---------
    kunnat_lista: lista, jossa Suomen kunnat.
    """
    kunnat_url = "https://fi.wikipedia.org/wiki/Luettelo_Suomen_kunnista"

    html_taulukot = pd.read_html(kunnat_url)
    kunnat_lista = html_taulukot[0]["Kunnan nimi"].to_list()
    return kunnat_lista


def oikotie_muunna_paivamaaraksi(ilmoitettu):
    """
    Funktio muuntaa Oikotien ilmoitusten div-tagien sisältämät tekstit päivämäärä-muotoon.

    Parametrit
    ----------
    ilmoitettu: bs4.element.Tag -tyyppinen muuttuja.

    Palauttaa
    ---------
    pvm: datetime
    """
    pvm = ilmoitettu.text.split()[1]
    pvm += str(dt.today().year)
    pvm = dt.strptime(pvm, "%d.%m.%Y")
    return pvm


def oikotie_nayta_lisaa(soup):
    """
    Funktio tarkistaa onko Oikotien sivulla listattujen paikkojen vanhin paikka maksimissaan viikon vanha.

    Parametrit
    ----------
    soup: bs4.BeautifulSoup-tyyppinen muuttuja.

    Palauttaa
    ---------
    True: jos vanhin näkyvillä oleva paikka ei ole yli viikkoa vanha.
    False: jos vanhin näkyvillä oleva paikka on yli viikon vanha.
    """
    ilmoitettu_pvmt = soup.find_all("div", {"class": "start-date"})
    print(soup)
    vanhin_nakyvilla = oikotie_muunna_paivamaaraksi(ilmoitettu_pvmt[-1])

    if vanhin_nakyvilla >= historia_pvm:
        return True
    else:
        return False


def tanaan():
    """
    Funktio muodostaa datetime.datetime tyyppisen muuttujan kuluvasta päivästä, jossa aikaan liittyvät osat ovat
    asetettu nolliksi.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    tanaan: datetime.datetime kuluvasta päivästä, jossa tunnit, minuutit, sekunnit ja mikrosekunnit ovat asetettu
    arvoon 0.
    """
    tanaan = dt.today().replace(hour=0, minute=0, second=0, microsecond=0)
    return tanaan


def monster():
    """
    Funktio hakee monster.fi palvelusta kaikki IT-paikat ja vie ne tietokantaan, jos ne eivät vielä ole siellä.
    Funktio hakee tietoja tarvittaessa niin monelta sivulta kuin on tarve.
    Haku koskee vain ennalta määritellyn ikäisiä ja tuoreempia paikkoja. Haku tehdään sivu kerrallaan, joten
    määriteltyä päivämäärää vanhempia ilmoituksia voi tulla kantaan max. 20 kpl. Tämä on tekijän hyväksymä "poikkeama".

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Työpaikkoihin liittyvät tiedot ovat listoissa / kaksiulotteisissa listoissa. Mahdolliset indeksivirheet otetaan
    niitä läpikäytäessä kiinni sekä ilmoitetaan missä indeksissä virhe ilmenee.

    Palauttaa
    ---------
    Funktio ei palauta mitään. Muodostaa tulosteen haun alkamisesta, päättymisestä sekä tietokantaan tallennettujen
    paikkojen lukumäärästä.
    """
    client = MongoClient()
    db = client.duunit
    duunit = db.duunit

    monster_domain = "https://www.monster.fi/tyopaikat/it"
    monster_it_url = "https://www.monster.fi/tyopaikat/it"
    monster_sivu = "?page="  # ?page=1 tarkoittaa sivua 2 (sivu 1 on pelkkä domain URL)
    lahde_monster = "Monster"

    # paikat_aluksi = duunit.count_documents({})
    paikat = []
    yritykset = []
    tyopaikat = []
    kunnat = []
    ilmoitettu = []

    sivunumero = 1
    j = 0

    print("Haetaan paikkoja palvelusta Monster...")

    while True:
        sivu = requests.get(monster_it_url)
        soup = BeautifulSoup(sivu.content, "html.parser")

        paikat.append(soup.find_all("a", {"class": "recruiter-job-link"}))
        yritykset.append(soup.find_all("span", {"class": "recruiter-company-profile-job-organization"}))
        tyopaikat_temp = [yritys.text for yritys in yritykset[j]]
        tyopaikat.append(tyopaikat_temp)
        kunnat.append(soup.find_all("div", {"class": "location"}))
        ilmoitettu.append(soup.find_all("span", {"class": "date"}))

        sivulta_tietokannassa = monster_sivulta_tietokannassa_lkm(paikat[j], tyopaikat[j], kunnat[j], lahde_monster)
        if sivulta_tietokannassa > 0:
            break

        seuraava_sivu = monster_seuraava_sivu(ilmoitettu[j])
        if seuraava_sivu:
            time.sleep(3)
            monster_it_url = monster_domain + monster_sivu + str(sivunumero)
            j += 1
            sivunumero += 1
        else:
            break

    i = 0
    k = 0

    kirjoitettuja = 0
    try:
        while True:
            for paikka in paikat[k]:
                if paikka.string is not None:
                    nimike = paikka.string
                    nimike = nimike.replace("&auml;", "ä").strip()
                    tyopaikka = tyopaikat[k][i].strip()
                    kunta = kunnat[k][i].text
                    kunta = kunta.replace("\n", "").strip()
                    pvm = dt.strptime(ilmoitettu[k][i].text.replace(",", "").strip(), "%d.%m.%Y")
                    linkki = paikka.get("href").strip()
                    i += 1
                else:
                    continue

                tulos = kirjoita_tietokantaan(nimike, tyopaikka, kunta, pvm, linkki, lahde_monster, duunit)
                if not tulos.matched_count:
                    kirjoitettuja += 1

            k += 1
            i = 0
            if k >= len(paikat):
                break
        print("Paikat haettu palvelusta Monster!")
        print(f"Monsterista löytyneet paikat: {kirjoitettuja}.")

    except IndexError as error:
        print("Virhe indeksissä: ", k, i)
        print(error)
        print()
        print("Yritä uudestaan. Virheen toistuessa, tarkista Monster-funktion koodi sekä sivun lähdekoodi muutoksien\
        varalta!")


def mol():
    """
    Funktio hakee mol.fi palvelusta kaikki IT-paikat ja vie ne tietokantaan, jos ne eivät vielä ole siellä. Jos
    dokumentti löytyy tietokannasta, se päivitetään.
    Haku koskee vain max viikon ikäisiä ja tuoreempia paikkoja.
    Kunta-informaatio voi olla eri kohdassa sivua eri ilmoituksissa. Mahdolliset tekstit tarkistetaan ja verrataan
    löytyykö se Wikipedian kuntalistauksesta. Jos ei löydy, merkitään ne "Paikkakunta ei tiedossa" -tekstillä.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään. Muodostaa tulosteen haun alkamisesta, päättymisestä sekä tietokantaan tallennettujen
    paikkojen lukumäärästä.
    """
    client = MongoClient()
    db = client.duunit
    duunit = db.duunit

    mol = "https://paikat.te-palvelut.fi"
    mol_it_url = "https://paikat.te-palvelut.fi/tpt/?professions=25%2C35&announced=3&leasing=0&remotely=0&english=false&sort=1"
    lahde_MOL = "MOL"
    ei_kuntaa = "Paikkakunta ei tiedossa."

    print("Haetaan paikkoja palvelusta MOL...")

    driver = lataa_chrome_ajuri()
    driver.get(mol_it_url)

    soup = BeautifulSoup(driver.page_source, "lxml")

    nimikkeet = soup.find_all("h4")
    if not nimikkeet:
        print("Paikkojen haku palvelusta MOL epäonnistui. Yritä myöhemmin uudestaan.")
        return
    nimikkeet = [nimike.text for nimike in nimikkeet]
    linkit = [mol + a["href"] for a in soup.find_all("a", href=True) if a["href"].startswith("/tpt")]

    kunnat_lista = hae_suomen_kunnat()

    ilmoitusten_div_tagit = soup.find_all("div", {"class": "col-xs-12 list-group-item"})
    pvm = tanaan()

    kirjoitettuja = 0
    # Ilmoitusten läpikäynti
    for i, div in enumerate(ilmoitusten_div_tagit):
        j = 0
        kaikki_span_tagit = div.find_all("span", attrs={"aria-label": True})
        # Yksittäisen ilmoituksen span-tagit läpi
        for span_tag in kaikki_span_tagit:
            # Ensimmäinen item on työpaikka
            if j == 0:
                tyopaikka = span_tag.text
            # Jos listan toinen item on kunta, lisätään työpaikalle paikkakunta
            elif j == 1:
                # vertailu kuntalistauksen kanssa
                if span_tag.text in kunnat_lista:
                    kunta = span_tag.text
                    tulos = kirjoita_tietokantaan(nimikkeet[i], tyopaikka, kunta, pvm, linkit[i], lahde_MOL, duunit)
                    if not tulos.matched_count:
                        kirjoitettuja += 1
                    break
            # Jos listan kolmas item on kunta, lisätään työpaikalle paikkakunta -
            # (kunta voi olla toinen tai kolmas, jos se ylipäätään on ilmoitettu)
            elif j == 2:
                if span_tag.text in kunnat_lista:
                    kunta = span_tag.text
                    tulos = kirjoita_tietokantaan(nimikkeet[i], tyopaikka, kunta, pvm, linkit[i], lahde_MOL, duunit)
                    if not tulos.matched_count:
                        kirjoitettuja += 1
                    break
                else:
                    kunta = ei_kuntaa
                    tulos = kirjoita_tietokantaan(nimikkeet[i], tyopaikka, kunta, pvm, linkit[i], lahde_MOL, duunit)
                    if not tulos.matched_count:
                        kirjoitettuja += 1
                    break

            j += 1

    driver.quit()
    print("Paikat haettu palvelusta MOL!")
    print(f"MOL:sta löytyneet paikat: {kirjoitettuja}.")


def lataa_chrome_ajuri():
    """
    Funktio muodostaa Chrome-ajurin käyttäen keksittyä/generoitua user agenttia, joka odottaa koko sivun latautumista
    sekä suorittaa Chromen taustalla piilotettuna.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    driver: selenium.webdriver.chrome.webdriver.WebDriver
    """
    ua = UserAgent()
    header = {"User-Agent": str(ua.chrome)}

    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=header")
    options.page_load_strategy = "normal"
    options.add_argument("--headless")

    driver = Chrome(options=options)
    return driver


def oikotie():
    """
    Funktio hakee oikotie.fi palvelusta kaikki IT-paikat ja vie ne tietokantaan, jos ne eivät vielä ole siellä. Jos
    dokumentti löytyy tietokannasta, se päivitetään.

    Haku koskee vain max viikon ikäisiä ja tuoreempia paikkoja. Toki mukaan voi tulla vanhempiakin ilmoituksia, koska
    näytä lisää painikkeen klikkaaminen tuo ison liudan paikkoja näkyville ja tässä otetaan kaikki näkyville tulevat
    mukaan eikä niitä karsita siinä vaiheessa päivämäärän mukaan. Tämä on tekijän hyväksymä poikkeus.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään. Muodostaa tulosteen haun alkamisesta, päättymisestä sekä tietokantaan tallennettujen
    paikkojen lukumäärästä.
    """
    client = MongoClient()
    db = client.duunit
    duunit = db.duunit

    oikotie = "https://tyopaikat.oikotie.fi/tyopaikat/suomi/it-tech"
    oikotie_domain = "https://tyopaikat.oikotie.fi"
    lahde_oikotie = "Oikotie"
    button_xpath = "/html/body/div[2]/div/div/div[1]/div/main/div/div/div/div/div[3]/div/div/div[2]/div[2]/div[1]/button"
    # button_class = "button-secondary"
    button_css = "span.text"
    paikat_aluksi = duunit.count_documents({})

    print("Haetaan paikkoja palvelusta Oikotie...")
    driver = lataa_chrome_ajuri()
    driver.get(oikotie)

    try:
        iframe = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "sp_message_iframe_609961")))
        driver.switch_to.frame(iframe)
        driver.find_element(By.XPATH, "//button[@title='Hyväksy kaikki evästeet']").click()
    except TimeoutException:
        driver.switch_to.default_content()
        print(f"Oikotie ei pyytänyt evästeiden hyväksymistä!")

    soup = BeautifulSoup(driver.page_source, "lxml")
    nayta_lisaa = oikotie_nayta_lisaa(soup)

    if nayta_lisaa:
        while True:
            try:
                # element_on_screen = driver.find_element(By.CSS_SELECTOR, button_css)
                # driver.execute_script("arguments[0].scrollIntoView();", element_on_screen)
                element = WebDriverWait(driver, 10) \
                    .until(EC.presence_of_element_located((By.XPATH, button_xpath))).click()
                # "//div[@class='show-more-button-container']/button[1]"))).click()
                # "//button[@title='Näytä lisää']"))).click()
            except ElementNotInteractableException:
                print("Oikotieltä ei voi hakea kuin n. 20 paikkaa. Etsitään korjausta asiaan.\
                 - ElementNotInteractableException")
                break
            except TimeoutException:
                print("Oikotieltä ei voi hakea kuin 20 paikkaa. Etsitään korjausta asiaan. - TimeoutException")
                break
            # Odotetaan, että uuden paikat ehtivät listautua näkyville
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, "lxml")

            if oikotie_nayta_lisaa(soup):
                continue
            else:
                break

    nimikkeet = soup.find_all("span", {"class": "text-clamped title"})
    nimikkeet = [nimike.text for nimike in nimikkeet]
    tyopaikat = soup.find_all("span", {"class": "employer"})
    tyopaikat = [tyopaikka.text for tyopaikka in tyopaikat]
    kunnat = soup.find_all("div", {"class": "locations-wrapper"})
    kunnat = [x.text.strip() for x in kunnat]
    ilmoitettu = soup.find_all("div", {"class": "start-date"})
    ilmoitettu = [oikotie_muunna_paivamaaraksi(pvm_teksti) for pvm_teksti in ilmoitettu]
    linkit = soup.find_all("a", {"class": "link-formatter job-ad-list-item-link"})
    linkit = [oikotie_domain + linkki["href"] for linkki in linkit]

    driver.quit()

    for nimike, tyopaikka, kunta, pvm, linkki in zip(nimikkeet, tyopaikat, kunnat, ilmoitettu, linkit):
        kirjoita_tietokantaan(nimike, tyopaikka, kunta, pvm, linkki, lahde_oikotie, duunit)

    print("Paikat haettu palvelusta Oikotie!")
    paikat_lopuksi = duunit.count_documents({})
    print(f"Oikotiellä uusia paikkoja yhteensä: {paikat_lopuksi - paikat_aluksi} kpl.")


def tulosta(paikka):
    """
    Funktio tulostaa työpaikan tiedot seuraavassa järjestyksessä allekkain:
    - Työnantaja
    - Nimike
    - Kunta
    - Ilmoituksen URL
    - Milloin työpaikkailmoitus on ilmoitettu / kirjoitettu tietokantaan.

    Jos tehtävässä esiintyy sana 'analytiikka', 'analyytikko', 'analyst', 'data' tai 'python',
    nimike tulostetaan vihreän taustavärin kera.


    Parametrit
    ----------
    paikka: dict, MongoDB:sta find-metodilla haettu yksittäisen paikan tiedot.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    korostus1 = "data"  # Tehtävänimikkeessä esiintyvät sana
    korostus2 = "python"
    korostus3 = "analytiikka"
    korostus4 = "analyytikko"
    korostus5 = "analyst"
    print(paikka["tyopaikka"])
    if korostus1 in paikka["nimike"].lower() or korostus2 in paikka["nimike"].lower() \
            or korostus3 in paikka["nimike"].lower() or korostus4 in paikka["nimike"].lower() \
            or korostus5 in paikka["nimike"].lower():
        print(Back.GREEN + paikka["nimike"] + Style.RESET_ALL)  # Tehtävänimikkeen tausta vihreäksi
    else:
        print(paikka["nimike"])
    print(paikka["kunta"])
    print(paikka["url"])
    print("ilmoitettu:", dt.strftime(paikka["ilmoitettu"], "%d.%m.%Y"))
    print()


def x_tuoreinta(lkm):
    """
    Funktio hakee MongoDB:sta x kpl tuoreimpia työpaikkoja joka palvelusta,
    lajittelee ne ilmoituspvm:n mukaan laskevaan järjesteykseen ja
    rajoittaa määrän parametrina saatavan arvon mukaiseksi.

    Haetut paikat tulostetaan tulosta-funktion avulla ruudulle.

    Parametrit
    ----------
    lkm: int, määrää kuinka monta paikkaa haetaan.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    x_tuoreinta_mol = list(duunit.find({"lähde": "MOL"}).sort("ilmoitettu", -1).limit(lkm))
    x_tuoreinta_monster = list(duunit.find({"lähde": "Monster"}).sort("ilmoitettu", -1).limit(lkm))
    x_tuoreinta_oikotie = list(duunit.find({"lähde": "Oikotie"}).sort("ilmoitettu", -1).limit(lkm))

    print(f"{lkm} tuoreinta työpaikkaa palvelusta MOL:\n")
    for paikka in x_tuoreinta_mol:
        tulosta(paikka)

    print(f"\n{lkm} tuoreinta työpaikkaa palvelusta Monster:\n")
    for paikka in x_tuoreinta_monster:
        tulosta(paikka)

    print(f"\n{lkm} tuoreinta työpaikkaa palvelusta Oikotie:\n")
    for paikka in x_tuoreinta_oikotie:
        tulosta(paikka)


def x_tuoreinta_mol(lkm):
    """
    Funktio hakee MongoDB:sta x kpl tuoreimpia työpaikkoja palvelusta MOL,
    lajittelee ne ilmoituspvm:n mukaan laskevaan järjesteykseen ja
    rajoittaa määrän parametrina saatavan arvon mukaiseksi.

    Haetut paikat tulostetaan tulosta-funktion avulla ruudulle.

    Parametrit
    ----------
    lkm: str, määrää kuinka monta paikkaa haetaan.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    x_tuoreinta_mol = list(duunit.find({"lähde": "MOL"}).sort("ilmoitettu", -1).limit(lkm))

    print(f"{lkm} tuoreinta työpaikkaa palvelusta MOL:\n")
    for paikka in x_tuoreinta_mol:
        tulosta(paikka)


def data_ja_python_paikat():
    """
    Funktio hakee MongoDB:sta kaikki paikat, joiden nimikkeeseen sisältyy sana 'data' tai sana 'python'.
    Haku on case-insensitive, eli sanan 'data' kirjainkoolla ei ole väliä, haku löytää sekä pienillä,
    että isoilla kirjaimilla olevat.

    Haetut paikat tulostetaan tulosta-funktion avulla ruudulle.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    d_ja_p_paikat = list(duunit.find({
        "$or": [{
            "nimike": {"$regex": "data", "$options": "i"}
        },
            {"nimike": {"$regex": "python", "$options": "i"}
             }]
    }).sort("ilmoitettu", -1))

    print(f"Tietokannassa olevat data- ja pythonpaikat:\n")
    if d_ja_p_paikat:
        for paikka in d_ja_p_paikat:
            tulosta(paikka)


def haku_sanalla(hakusana):
    """
    Funktio hakee MongoDB:sta kaikki paikat, joiden työnantaja, nimike, tai kunta sisältää hakusanan.
    Haku on case-insensitive, eli sanan kirjainkoolla ei ole väliä, haku löytää sekä pienillä, että isoilla
    kirjaimilla olevat.

    Haetut paikat tulostetaan tulosta-funktion avulla ruudulle.

    Parametrit
    ----------
    hakusana: str

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    paikat = list(duunit.find({
        "$or": [{
            "nimike": {"$regex": hakusana, "$options": "i"}
        },
            {
                "kunta": {"$regex": hakusana, "$options": "i"}
            },
            {
                "tyopaikka": {"$regex": hakusana, "$options": "i"}
            }]
    }).sort("ilmoitettu", -1))

    if paikat:
        print(f"Hakusanalla '{hakusana}' löytyi seuraavat työpaikat:\n")
        for paikka in paikat:
            tulosta(paikka)
    else:
        print(f"Hakusanalla '{hakusana}' ei löytynyt yhtään työpaikkaa!")


def tyopaikkojen_lkm():
    """
    Funktio laskee tietokannassa olevien työpaikkojen lukumäärän ja antaa tulosteen lukumäärästä ruudulle.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio palauttaa MongoDB:ssa olevien työpaikkojen lukumäärän ja tekee siitä myös tulosteen.
    """
    tyopaikkoja = duunit.count_documents({})
    print(f"Tietokannassa on {tyopaikkoja} työpaikkaa.")
    return tyopaikkoja


def paivan_paikat():
    """
    Funktio hakee tänään MongoDB:een tallennetut työpaikat, jotka on noudettu palveluista Monster sekä Oikotie
    ja tulostaa ne tulosta-funktion avulla ruudulle aakkosjärjestyksessä.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    pvm = tanaan()
    paikat = list(duunit.find({"ilmoitettu": {"$eq": pvm}, "lähde": {"$in": ["Monster", "Oikotie"]}}))

    if paikat:
        print(f"Tämän päivän työpaikat (Monster & Oikotie):\n")
        for paikka in paikat:
            tulosta(paikka)
    else:
        print(f"Tietokannassa ei löytynyt yhtään tänään tallennettua työpaikkailmoitusta.")


def max_x_paivaa_vanhat(x_paivaa):
    """
    Funktio hakee tänään MongoDB:een tallennetut työpaikat ja tulostaa ne tulosta-funktion avulla ruudulle
    aikajärjestyksessä, tuoreimmasta vanhimpaan.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen.
    """
    try:
        x_paivaa = int(x_paivaa)
        pvm = tanaan()
        historia_pvm = pvm - td(x_paivaa)
        paikat = list(duunit.find({"ilmoitettu": {"$gte": historia_pvm}, "lähde": {"$in": ["Monster", "Oikotie"]}})
                      .sort("ilmoitettu", -1))

        print(f"Max {x_paivaa} päivää vanhat työpaikkailmoitukset:\n")
        for paikka in paikat:
            tulosta(paikka)

    except ValueError as e:
        print("Syötit virheellisen arvon! Ole hyvä ja yritä uudelleen.")


def poista_yli_30_paivaa_vanhat():
    """
    Funktio poistaa yli 30 päivää vanhat paikat tietokannasta.

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään. Poistettujen dokumenttien lukumäärä ilmoitetaan funktion muodostamassa tulosteessa.
    """
    ennen_poistoa = duunit.count_documents({})

    vanhat_raja = tanaan() - td(30)

    duunit.delete_many({"ilmoitettu": {"$lt": vanhat_raja}})

    poiston_jalkeen = duunit.count_documents({})

    print(f"Tietokannasta poistettiin {ennen_poistoa - poiston_jalkeen} työpaikkaa.")


def virheilmoitus_int():
    print("Yritä uudestaan syöttämällä positiivinen kokonaisluku!")


def tarkistin(luku):
    if luku < 1:
        print("Luvun tulee olla vähintään yksi, ole hyvä ja yritä uudestaan.")
        return False
    return True


def lokiasetukset():
    logging.basicConfig(filename=lokitiedosto, level=logging.INFO)


def ajoaika_lokitiedostoon(ajon_tyyppi, kesto, tyopaikkoja_lisatty):
    """
    Funktio kirjoittaa lokitiedostoon tiedon, onko kyseessä tavallinen jaksottainen ajo tai rinnakkaisajo,
    ajon keston sekä montako työpaikkaa kyseisellä kerralla vietiin tietokantaan.

    Parametrit
    ----------
    ajon_tyyppi: str
    kesto: int
    tyopaikkoja_listatty: int

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan yhden rivin lokitiedostoon.
    """
    viesti = ajon_tyyppi + "," + str(kesto) + "," + str(tyopaikkoja_lisatty)
    logging.info(viesti)


def lue_lokia_df_muodossa():
    """
    Funktio lukee lokitiedoston dataframeen ja muokkaa ajotapa-saraketta. Lopuksi dataframe tulostetaan ruudulle.
    Jos lokitiedostoa ei ole olemassa, niin tulostaa ilmoituksen tästä ruudulle..

    Parametrit
    ----------
    Ei parametreja.

    Virheilmoitukset
    ----------------
    Ei ole käytetty Try-Except lauseita.

    Palauttaa
    ---------
    Funktio ei palauta mitään, tekee ainoastaan tulosteen ruudulle.
    """
    loki = loki_df()
    print(loki)


def loki_df():
    if os.path.exists(lokitiedosto):
        loki_df = pd.read_csv(lokitiedosto, names=["ajotapa", "aika", "tyopaikkoja"])
        loki_df["ajotapa"] = loki_df["ajotapa"].str[10:]
        return loki_df
    else:
        print("Lokitiedostoa ei ole olemassa!")

def visualisointi():
    loki = loki_df()
    loki["aika"] = loki["aika"].astype("float")
    gr_loki = loki.groupby(by="ajotapa")["aika"].mean()
    ax = gr_loki.plot(kind="bar", x="ajotapa", y="aika", title="Multiprosessointi vs tavallinen - keskiarvo",
                      xlabel="", ylabel="sekuntia", rot=0)
    ax.grid("on", axis="y")
    ax.set_axisbelow(True)
    plt.show()

def main():
    haku = input("Haetaanko uudet paikat (k/e): ")
    if haku in ["k", "K"]:
        tyopaikkojen_lkm_aluksi = tyopaikkojen_lkm()
        nopeutettu = input("Haetaanko paikat käyttäen rinnakkaisajoa (k/e): ")
        print("Haetaan uudet paikat palveluista MOL ja Monster!\n")
        aloitusaika = time.perf_counter()
        if nopeutettu in ["k", "K"]:
            p_mol = Process(target=mol)
            p_monster = Process(target=monster)
            # p_oikotie = Process(target=oikotie)
            p_mol.start()
            p_monster.start()
            # p_oikotie.start()
            p_mol.join()
            p_monster.join()
            # p_oikotie.join()
        else:
            mol()
            monster()
            # oikotie()
        lopetusaika = time.perf_counter()
        kesto = lopetusaika - aloitusaika

        print(f"Haku kesti: {kesto:.2f} sekuntia.\n")
        ajon_tyyppi = "multi" if nopeutettu in ["k", "K"] else "normi"
        tyopaikkojen_lkm_lopuksi = tyopaikkojen_lkm()
        tyopaikkoja_lisatty = tyopaikkojen_lkm_lopuksi - tyopaikkojen_lkm_aluksi
        print(f"Työpaikkoja haun jälkeen: {tyopaikkojen_lkm_lopuksi} kpl")
        lokiasetukset()
        ajoaika_lokitiedostoon(ajon_tyyppi, kesto, tyopaikkoja_lisatty)
        logging.shutdown()

    tyhjenna = False
    while True:
        if tyhjenna:
            input("Paina enteriä jatkaaksesi:")
            clear_output(wait=True)
        print("\n\nTyöpaikkahaku")
        print(50 * "-")
        print("1. - x tuoreinta paikkaa per palvelu")
        print("2. - Tämän päivän paikat palveluista Monster ja Oikotie")
        print("3. - Max x päivää vanhat paikat palveluista Monster ja Oikotie (esim. 5 = max 5 pvää vanhat paikat)")
        print("4. - MOL-palvelun x tuoreinta paikkaa")
        print("5. - Haku sanalla (kohdistuu työnantajaan, työpaikkaan sekä kuntaan)")
        print("6. - Data, Python ja analytiikka-paikat")
        print("7. - Kannassa olevien työpaikkojen lukumäärä")
        print("8. - Poista yli 30 vrk vanhat paikat")
        print("9. - Lue suorituskykylokia")
        print("10. - Suorituskyky visualisointina")
        print("Q. - Lopetus")
        print()
        valinta = input("Valintasi on: ")
        tyhjenna = True
        if valinta == "1":
            try:
                x_kpl = int(input("Kuinka monta uusinta paikkaa haluat katsoa per palvelu: "))
                if not tarkistin(x_kpl):
                    continue
                x_tuoreinta(x_kpl)
            except ValueError:
                virheilmoitus_int()
        elif valinta == "2":
            paivan_paikat()
        elif valinta == "3":
            try:
                x_paivaa = int(input("Kuinka monta päivää vanhat ilmoitukset sisällytetään hakuun: "))
                if not tarkistin(x_paivaa):
                    continue
                max_x_paivaa_vanhat(x_paivaa)
            except ValueError:
                virheilmoitus_int()
        elif valinta == "4":
            try:
                x_kpl = int(input("Kuinka monta uusinta paikkaa haluat katsoa palvelusta MOL: "))
                if not tarkistin(x_kpl):
                    continue
                x_tuoreinta_mol(x_kpl)
            except ValueError:
                virheilmoitus_int()
        elif valinta == "5":
            hakusana = input("Kirjoita hakusana: ")
            haku_sanalla(hakusana)
        elif valinta == "6":
            data_ja_python_paikat()
        elif valinta == "7":
            tyopaikkojen_lkm()
        elif valinta == "8":
            poista_yli_30_paivaa_vanhat()
        elif valinta == "9":
            lue_lokia_df_muodossa()
        elif valinta == "10":
            visualisointi()
        elif valinta in ["Q", "q"]:
            print(25 * "*")
            print("Ohjelma lopetetiin!")
            print(25 * "*")
            break


if __name__ == "__main__":
    main()

# Ei tallentanut kantaan kuin vain oikotien paikat - ei monsterin! - ei toimi vieläkään moniajolla.
# Vei kai vasta viimeisenä haetut kantaan!!!


# Tämän päivän paikat - näyttää liikaa, mutta johtuu siitä, jos ilmoituksen pvm on päivitetty sivulla, niin päivittää
# ilmoituksen tietokannassa - eli näyttää ilmoituksen tuoreimman version
