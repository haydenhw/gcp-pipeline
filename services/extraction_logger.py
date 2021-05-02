import logging

def log_current_year(year):
  logging.basicConfig(filename="extractions.log", level=logging.INFO)
  logging.info(year)


def get_last_year_processed():
    with open("extractions.log") as f:
      lines = f.readlines()
      last_line = lines[-1]
      year = last_line.split(":")[-1]
      return year