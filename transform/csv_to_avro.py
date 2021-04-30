import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os


def write_avro(file_in, file_out, schema_path):
    schema = avro.schema.parse(open(schema_path, "rb").read())
    writer = DataFileWriter(open(file_out, "wb"), DatumWriter(), schema)
    with open(file_in) as f_in:
        for line in f_in:
            line = line.split(",")
            print("CSV LINE: ", line)
            writer.append({"name": line[0], "sex": line[1], "count": int(line[2]), "year": int(line[3])})
    writer.close()

def read_avro(file_out):
    reader = DataFileReader(open(file_out, "rb"), DatumReader())
    for line in reader:
        print(line)
    reader.close()

if __name__ == "__main__":
    file_in = os.path.join("data", "names", "yob2016.txt")
    file_out = os.path.join("data", "names", "yob2016.avro")
    schema_path = os.path.join("data", "yob.avsc")
    write_avro(file_in, file_out, schema_path)
    read_avro(file_out)
