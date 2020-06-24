import requests
from lxml import etree
from concurrent.futures import ThreadPoolExecutor
import csv
import time

good = []
bad = []


def readNIPs(filename):
    data = [line.rstrip() for line in open(filename)]

    return data


def generateeori(nip):
    return 'PL'+nip+'00000'


def getdata(nip):
    print("Scraping %s" % nip)
    url = 'https://ec.europa.eu/taxation_customs/dds2/eos/eori_detail.jsp?Lang=pl&EoriNumb='+generateeori(nip)
    data = requests.get(url=url)
    data.encoding = "utf-8"
    return data.text


def parsehtml(page, nip):
    myparser = etree.HTMLParser(encoding="utf-8")
    tree = etree.HTML(page, parser=myparser)

    date = tree.xpath("//tr[./td[./b[text()='Data wniosku:']]]/td[2]/text()")
    name = tree.xpath("//tr[./td[./b[text()='Nazwa / Imię i nazwisko']]]/td[2]/text()")
    address = tree.xpath("//tr[./td[./b[text()='Adres']]]/td[2]/text()")
    street = tree.xpath("//tr[./td[./b[text()='Street number']]]/td[2]/text()")
    postalcode = tree.xpath("//tr[./td[./b[text()='Postal code']]]/td[2]/text()")
    city = tree.xpath("//tr[./td[./b[text()='Miasto']]]/td[2]/text()")
    country = tree.xpath("//tr[./td[./b[text()='Kraj']]]/td[2]/text()")

    if len(name) > 0:
        return {'NIP': nip, 'Data wniosku': date[0], 'Nazwa': name[0], 'Adres': address[0], 'Street number': street[0],
                'Postal code': postalcode[0], 'Miasto': city[0], 'Kraj': country[0]}
    else:
        return "Bad"


def scraper(nip):
    global good, bad

    html = getdata(nip)
    data = parsehtml(html, nip)

    if data == "Bad":
        bad.append(nip)
    else:
        good.append(data)


def savecsv():
    global good
    with open('good.csv', mode='w') as csv_file:
        fieldnames = ['NIP', 'Data wniosku', 'Nazwa', 'Adres', 'Street number', 'Postal code', 'Miasto', 'Kraj']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        writer.writeheader()
        for value in good:
            writer.writerow(value)


def savebad():
    global bad
    with open('bad.txt', 'w') as f:
        for item in bad:
            f.write("%s\n" % item)


def start():
    nips = readNIPs('NIP.txt')
    start = time.time()
    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(scraper, nips)
    savecsv()
    savebad()
    end = time.time()
    msg = 'Operation took {:.3f} seconds to complete.'
    print(msg.format(end - start))


start()