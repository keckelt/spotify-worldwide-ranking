# Author: Eduardo Mucelli Rezende Oliveira
# E-mail: edumucelli@gmail.com

import os
from datetime import datetime, timedelta, date
import csv
import threading

from tqdm import tqdm
import requests

DATA_DIRECTORY = 'data'


class Collector(threading.Thread):
    def __init__(self, region, start_date, end_date):
        super(Collector, self).__init__()
        self.region = region
        self.start_date = start_date
        self.end_date = end_date
        self.base_headers = ['Position', 'Track Name', 'Artist', 'Streams', 'URL']

    def date_range(self):
        one_day = timedelta(days=1)
        current_date = self.start_date
        while current_date <= self.end_date:
            yield current_date
            current_date += one_day

    def is_csv_ok(self, download_content):
        csv_reader = csv.reader(download_content.splitlines(), delimiter=',')
        note = csv_reader.next()
        headers = csv_reader.next()
        return set(headers) == set(self.base_headers)

    def download_csv_file(self, url):
        with requests.Session() as session:
            session.headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'de-AT,en-US;q=0.7,en;q=0.3',
                                    'Connection': 'keep-alive',
                # 'Cookie': '__cfduid=d4176377ae4eee1c229ee921ae76268891572271290; X-Mapping-kjhgfmmm=9343FCBF8CF03218178FBD7548D564F1; XSRF-TOKEN=eyJpdiI6IjhkT3dpbEVpbG52cGIwYVMrS0hHdXc9PSIsInZhbHVlIjoiN0VDZ2F1V3JHYnBMYWROK1FUSWRtZUZSelhmaUJCNkNkb3U0UllMeUVNOTkwQVQ1MlBGT3VKMlwvTzBnQ1dOc2pMVUZLWmR0MEwzMG1KR0NMaElrSFF3PT0iLCJtYWMiOiJkZmY5ZThjNzJhYTU3MDNmMmQyMWYyM2U4NTg1OGY0MmE4OWRiYjU1YzBmZGVmODM5N2U4MmMyMTBlMWYxZGRjIn0%3D; laravel_session=eyJpdiI6IlhGSHZoUFlWbE5aV2UwbWJoQ0NEMUE9PSIsInZhbHVlIjoiZHo2RlFydDRKQnBIZ1wvUFpyV1ZLelpaMGVadHNRdUhmQmJzS2JZa2ZaNnhpaEJZWlNmckR6eFdFbjBrRTR2dGtkY3NKNnRkSGp6ak1GdmEyNFJqcVZBPT0iLCJtYWMiOiJhNjc0NDYxNzIyNjZjYzRkN2IxYWJkNDg2YzAyOTQ1MzI5OTM4OTEyMzIzODY0ZjkzYjdkYWJlMjA1MWJmMDhjIn0%3D; d53e837420ae192d2eacc8608e11e0cc0c2087ed=eyJpdiI6ImpMaTQ0b1RKdWY0TzRwMExqS3JYcUE9PSIsInZhbHVlIjoiT1dvNCtUZ3Fqa2I4ZnZhSXBFMDVwV1Z6N1pQdXo3ZVphQUcwTHRYMVwvMTZ0WHNYRVwvaUNcL29qWXFaWUlXVkJlRm8rMWhheXNTRWU1elpwcEJUVzJ5eFptXC9NMUVCUlVOOFpvQ3lOeUg5OWJnVVwveXB0c1J0cnN0dUF3cDFJdGdwTHZtR0V1MkZ6TFZwcWtWRGdtSVdMSXkrOEJYQXdnTlFvMkgyRmd2QnZVTFVHWitjNlZ2TGRpdjhMSXBwZ09PUEdrMllxTVVOZk9zV1dLUHVuVXVHQXVaU3UxK2RQOXpkMjAwRzdoeE9acFQxT0Q3SUx3TThCVkgrdXlzU3dIcTN1RkZ2cVYwMW9HUTg4S1dlNFArS21wbTVMaVZaTFRYK1NiTXd1YkVscW0wSG9mNFNUUXJ2dUI2S0hPQ1dBdnVSTVgreDY4V0N3XC9PcG1yU09ZMmZJV2pTVXVKZFpWNWFUWGJVN1JhTWwrSVRKSlVPbnc0M3c2M3R5eW5od05xVHh6cEs3Uys0WFdpWEZPcFwvRW11OHVXaWU5YTFXbEpUSUVzcmtFcEw0dTkxWXJoTVFVbGJxMVN3RGkwdkdURGZ6bGtoaEQ0VGUreUEwKzBDUEh6dlZXVGcwZ0JZTzZVK2M4djFZbkFKN2gzZUI0PSIsIm1hYyI6IjYxNzRhNjIxZjQ5ZjExNzIxMjYzNGMyYTcwZjM5ZmJmN2ZiNzVkNDM4Mjc5OGYyN2E0YzI2NzQ3NmIzMTBlNGQifQ%3D%3D',
                                    'Host': 'spotifycharts.com',
                                    'Referer': 'https://spotifycharts.com/regional/ad/weekly/latest',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) Gecko/20100101 Firefox/70.0'
                })
            retry = 3
            while True:
                download = session.get(url)
                if self.is_csv_ok(download.content):
                    return download.content
                print "Retrying for '%s'" % url
                retry -= 1
                if retry <= 0:
                    print "Retry failed for '%s'" % url
                    return None

    def extract_csv_rows(self, csv_file):
        csv_reader = csv.reader(csv_file.splitlines(), delimiter=',')
        csv_reader.next() # skip note
        csv_reader.next() # skip header
        for row in csv_reader:
            yield row

    def run(self):
        if not os.path.exists(DATA_DIRECTORY):
            os.makedirs(DATA_DIRECTORY)

        headers = self.base_headers + ["Date", "Region"]

        file_path = os.path.join(DATA_DIRECTORY, "%s.csv" % self.region)
        if os.path.exists(file_path):
            print "File '%s' already exists, skipping" % file_path
        else:
        with open(file_path, 'wb', 1) as out_csv_file:
            writer = csv.writer(out_csv_file)
            writer.writerow(headers)

            for current_date in tqdm(self.date_range(), desc="Collecting from '%s'" % self.region):
                url = "https://spotifycharts.com/regional/%s/daily/%s/download" % (self.region, current_date)
                csv_file = self.download_csv_file(url)
                if csv_file is None:
                    continue

                for row in self.extract_csv_rows(csv_file):
                    row.extend([current_date, self.region])
                    writer.writerow(row)

    @staticmethod
    def generate_final_file():
        final_filename = 'data.csv'

        with open(final_filename, 'w') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(['Position', 'Track Name', 'Artist', 'Streams', 'URL'])
            for filename in tqdm(os.listdir(DATA_DIRECTORY), desc="Generating final file: %s" % final_filename):
                if filename.endswith(".csv"):
                    with open(os.path.join(DATA_DIRECTORY, filename)) as infile:
                        csv_reader = csv.reader(infile)
                        csv_reader.next() # skip header
                        for row in csv_reader:
                            csv_writer.writerow(row)


if __name__ == "__main__":

    one_day = timedelta(days=1)
    start_date = date(2017, 1, 1)
    end_date = datetime.now().date() - (2 * one_day)

    regions = ["global", "us", "gb", "ad", "ar", "at", "au", "be", "bg",
               "bo", "br", "ca", "ch", "cl", "co", "cr", "cy", "cz", "de",
               "dk", "do", "ec", "ee", "es", "fi", "fr", "gr", "gt", "hk",
               "hn", "hu", "id", "ie", "is", "it", "jp", "lt", "lu", "lv",
               "mc", "mt", "mx", "my", "ni", "nl", "no", "nz", "pa", "pe",
               "ph", "pl", "pt", "py", "se", "sg", "sk", "sv", "tr", "tw", "uy"]

    for region in regions:
        collector = Collector(region, start_date, end_date)
        collector.start()
    Collector.generate_final_file()
