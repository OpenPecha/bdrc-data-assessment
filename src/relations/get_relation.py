import csv


def extract_last_part_of_predicate(csv_file_path):
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        print("Extracted Predicates:")
        for row in reader:
            predicate = row['predicate']
            last_part = predicate.split('/')[-1].split('#')[-1]
            print(last_part)


csv_file_path = 'data/relatioship.csv'

extract_last_part_of_predicate(csv_file_path)
