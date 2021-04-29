# read file name
# get year
# iterate over csv data
# append year to end
import glob

if __name__ == "__main__":
    data_dir = "data/"
    staging_dir = "staging/"

    for yob_f in glob.glob(data_dir + "yob*.txt"):
        year = yob_f .split(".")[0][-4:]
        with open(yob_f) as f:
            with open(f"{staging_dir}yob{year}.txt", "w") as f_out:
                for line in f:
                    updated_line = f"{line.strip()},{year}\n"
                    print(updated_line)
                    f_out.write(updated_line)


