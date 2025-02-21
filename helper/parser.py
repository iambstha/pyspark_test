
from pyspark.sql import Row #type: ignore

def parse_fixed_width(line):
    return Row(
        id=line[0:9].strip(),
        first_name=line[9:30].strip(),
        last_name=line[30:51].strip(),
        address=line[51:82].strip(),
        city=line[82:103].strip(),
        state=line[103:110].strip(),
        zip_code=line[110:115].strip().zfill(5)
    )