size_in_gb = int(input("What size file do you need? "))

TARGET_SIZE = size_in_gb * 1024 * 1024 * 1024
FILE_NAME = "large_fixed_width_data.txt"

ID_WIDTH = 8
NAME_WIDTH = 20
SURNAME_WIDTH = 20
ADDRESS_WIDTH = 30
CITY_WIDTH = 20
STATE_WIDTH = 6
ZIP_WIDTH = 10

sample_records = [
    ("John", "Doe", "123 Main St", "Anytown", "NY", "12345"),
    ("Jane", "Smith", "456 Elm St", "Otherville", "CA", "54321"),
    ("Michael", "Johnson", "789 Oak St", "Smalltown", "TX", "67890"),
    ("Emily", "Brown", "101 Pine St", "Countryside", "FL", "13579"),
    ("William", "Taylor", "246 Maple St", "Suburbia", "OH", "24680")
]

def format_record(record_id, first, last, address, city, state, zip_code):
    return (
        str(record_id).rjust(ID_WIDTH) + " " +
        first.ljust(NAME_WIDTH) + " " +
        last.ljust(SURNAME_WIDTH) + " " +
        address.ljust(ADDRESS_WIDTH) + " " +
        city.ljust(CITY_WIDTH) + " " +
        state.ljust(STATE_WIDTH) + " " +
        zip_code.ljust(ZIP_WIDTH) + "\n"
    )

num_repeats = TARGET_SIZE // (len(format_record(1, *sample_records[0]).encode("utf-8")) * len(sample_records))

print(f"Approximate maximum ID: {num_repeats * len(sample_records)}")

with open(FILE_NAME, "w") as f:
    record_id = 1
    for _ in range(num_repeats):
        for record in sample_records:
            f.write(format_record(record_id, *record))
            record_id += 1

print(f"Fixed-width file '{FILE_NAME}' of approximately {TARGET_SIZE / (1024**3)}GB generated successfully.")
