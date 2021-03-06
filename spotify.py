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

    def next_date(self):
        one_day = timedelta(days=1)
        current_date = self.start_date
        while current_date <= self.end_date:
            yield current_date
            current_date += one_day

    def date_range(self):
        total = (self.end_date - self.start_date).days
        return total

    def is_csv_ok(self, download_content):
        try:
            # print('%s' % download_content)
            csv_reader = csv.reader(download_content.splitlines(), delimiter=',')
            note = next(csv_reader) # ,,,"Note that these figures are generated using a formula that protects against any artificial inflation of chart positions.",
            headers = next(csv_reader)
            return set(headers) == set(self.base_headers)
        except:
            print('csv invalid - missing data?')
            return False

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

            download = session.get(url)
            if self.is_csv_ok(download.text):
                return download.text
            else:
                return None

    def extract_csv_rows(self, csv_file):
        csv_reader = csv.reader(csv_file.splitlines(), delimiter=',')
        next(csv_reader) # skip note
        next(csv_reader) # skip header
        for row in csv_reader:
            yield row

    def run(self):
        if not os.path.exists(DATA_DIRECTORY):
            os.makedirs(DATA_DIRECTORY)

        headers = self.base_headers + ["Date", "Region"]

        file_path = os.path.join(DATA_DIRECTORY, "%s.csv" % self.region)

        if os.path.exists(file_path):
            print("File '%s' already exists, skipping" % file_path)
        else:
            with open(file_path, 'w', 1) as out_csv_file:
                writer = csv.writer(out_csv_file)
                writer.writerow(headers)

                for current_date in tqdm(self.next_date(), total=self.date_range(), desc="Collecting from '%s'" % self.region):
                    url = "https://spotifycharts.com/regional/%s/daily/%s/download" % (self.region, current_date)
                    csv_file = self.download_csv_file(url)
                    if csv_file is None:
                        continue

                    for row in self.extract_csv_rows(csv_file):
                        row.extend([current_date, self.region])
                        writer.writerow(row)

    @staticmethod
    def generate_final_file():
        final_filename = 'merged_data.csv'

        with open(final_filename, 'w') as outfile:
            csv_writer = csv.writer(outfile)
            csv_writer.writerow(['Position', 'Track Name', 'Artist', 'Streams', 'URL', 'Date', 'Country'])
            for filename in tqdm(os.listdir(DATA_DIRECTORY), desc="Generating final file: %s" % final_filename):
                if filename.endswith(".csv"):
                    with open(os.path.join(DATA_DIRECTORY, filename)) as infile:
                        csv_reader = csv.reader(infile)
                        next(csv_reader) # skip header
                        for row in csv_reader:
                            csv_writer.writerow(row)


if __name__ == "__main__":
    start_date = date(2020, 1, 1)

    one_day = timedelta(days=1)
    #end_date = datetime.now().date() - (one_day) # Skip today
    end_date = date(2020, 12, 31)

    # regions = ["gb", "ad", "at", "be", "bg",
    #            "ch", "cy", "cz", "de",
    #            "dk", "ee", "es", "fi", "fr", "gr",
    #            "hu", "ie", "is", "it", "lt", "lu", "lv",
    #            "mc", "mt", "nl", "no",
    #            "pl", "pt", "ro", "se", "sk", "tr"]
    regions = ["at", "be", "ch", "cz", "de", "dk", "hu", "lu", "nl", "pl", "sk"] #subset

    for region in regions:
        collector = Collector(region, start_date, end_date)
        collector.start()

    Collector.generate_final_file()
    # Run script twice, first time it will download the countries, second time it will update the merged_data.csv file
