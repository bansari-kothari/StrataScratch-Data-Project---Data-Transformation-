# This is a Python script to transform JSON data to create 3 CSV files.

# Problem Statement: https://platform.stratascratch.com/data-projects/data-transformation?tabname=assignment

import json
import csv
import pandas as pd


def write_to_file(name, list_data):
    is_header = True
    data_file = open(name, 'w', newline='')
    csv_writer = csv.writer(data_file)
    for list_row in list_data:
        if is_header:
            header = list_row.keys()
            csv_writer.writerow(header)
            is_header = False
        csv_writer.writerow(list_row.values())
    data_file.close()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    with open('case.json') as json_file:
        json_data = json.load(json_file)

    dynamicPriceRange = []
    dynamicPriceOption = []
    curatedOfferOptions = []
    for data in json_data:

        time = pd.to_datetime(data['EnqueuedTimeUtc']).tz_convert(tz="Brazil/East").strftime('%d/%m/%Y')
        payload = json.loads(data['Payload'])
        if data['EventName'] == 'DynamicPrice_Result':
            if payload['provider'] == 'ApplyDynamicPriceRange':
                algoOp = payload['algorithmOutput']
                row = {
                    'Provider': payload['provider'],
                    'OfferId': payload['offerId'],
                    'MinGlobal': algoOp['min_global'],
                    'MinRecommended': algoOp['min_recommended'],
                    'MaxRecommended': algoOp['max_recommended'],
                    'DifferenceMinRecommendMinTheory': algoOp['differenceMinRecommendMinTheory'],
                    'EnqueuedTimeSP': time
                }
                dynamicPriceRange.append(row)
            elif payload['provider'] == 'ApplyDynamicPricePerOption':
                algoOp = payload['algorithmOutput']
                for option_price in algoOp:
                    row = {
                        'Provider': payload['provider'],
                        'OfferId': payload['offerId'],
                        'UniqueOptionId': option_price['uniqueOptionId'],
                        'BestPrice': option_price['bestPrice'],
                        'EnqueuedTimeSP': time
                    }
                    dynamicPriceOption.append(row)
        elif data['EventName'] == 'CurateOffer_Result':
            for record in payload:
                for option in record['options']:
                    row = {'CurationProvider': record['curationProvider'], 'OfferId': record['offerId'],
                           'DealerId': record['dealerId'], 'UniqueOptionId': option['uniqueOptionId'],
                           'OptionId': option['optionId'], 'IsMobileDealer': option['isMobileDealer'],
                           'IsOpen': option['isOpen'], 'Eta': option['eta'], 'ChamaScore': option['chamaScore'],
                           'ProductBrand': option['productBrand'], 'IsWinner': option['isWinner'],
                           'MinimumPrice': option['minimumPrice'], 'MaximumPrice': option['maximumPrice'],
                           'DynamicPrice': option['dynamicPrice'], 'FinalPrice': option['finalPrice'],
                           'DefeatPrimaryReason': option['defeatPrimaryReason'] if 'defeatPrimaryReason' in option else "",
                           'DefeatReasons': ', '.join(option['defeatReasons']) if 'defeatReasons' in option else "",
                           'EnqueuedTimeSP': time
                           }

                    curatedOfferOptions.append(row)

    write_to_file('CuratedOfferOptions.csv', curatedOfferOptions)
    write_to_file('DynamicPriceOption.csv', dynamicPriceOption)
    write_to_file('DynamicPriceRange.csv', dynamicPriceRange)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
