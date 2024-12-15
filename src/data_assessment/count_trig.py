import os


def count_trig_files(directory):
    trig_file_count = 0

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.trig'):
                trig_file_count += 1

    return trig_file_count


if __name__ == "__main__":
    directory_path = "data/instances-20220922"

    count = count_trig_files(directory_path)
    print(f"total .trig files: {count}")
