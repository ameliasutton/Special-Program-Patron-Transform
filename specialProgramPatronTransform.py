import json
import pandas
from datetime import datetime
import logging
import dotenv
import os


class PatronDataTransformer:
    def __init__(self, config_name, time):
        logging.info("Initializing patron data converter...")
        self.config_file_name = config_name

        # Read patron file
        try:
            logging.info('Reading patron file... \"%s\"...',
                         os.getenv('patronFileName'))
            self.patron_CSV = pandas.read_csv(
                os.getenv('patronFileName'), delimiter=",", dtype="string")
        except FileNotFoundError as excep:
            logging.critical('Staff load file, \"%s\", not found',
                             os.getenv('staffFileName'))
            raise FileNotFoundError from excep

        # Initializes blank Output file dict
        self.patron_out = []

        self.time = time
        self.patron_out_file_name = f'{os.getenv("destinationFolder")}{os.getenv("patronFileName")[:-4]}.json'
        print(self.patron_out_file_name)

        logging.info('Prepared load file will be saved as: %s',
                     self.patron_out_file_name)
        self._logElapsedTime()
        logging.info("Patron data converter initialized\n")

    # Logs time elapsed since the time passed into the object on initialization
    def _logElapsedTime(self):
        time_now = datetime.now()
        elapsed_time = time_now - self.time
        logging.info('Total elapsed time (seconds): %s', elapsed_time.seconds)

    # Converts Staff records to FOLIO's json format and saves it in the output file
    def transformPatronRecords(self):
        if not self.patron_CSV.keys().tolist():
            logging.warning(
                "Patron file contains no records, output file will contain no data\n")
            return -1

        logging.info("Converting Patron records to json...\n")

        for row in self.patron_CSV.itertuples():
            patron = row._asdict()
            # Assigns Patron Group (REU should get Undergrad, Donahue should get Staff)
            patron_group = "Undergraduate"

            if patron["Email_Address"] is None:
                email = str(patron["ID"]) + "@umass.edu"
            else:
                email = patron["Email_Address"]

            try:
                expiration_date = patron["Expiration_Date"]
            except: 
                expiration_date = '2023-07-31'

            patron_json = {
                "username": str(patron['ID']) + '@umass.edu',
                "externalSystemId": str(patron["ID"]) + "@umass.edu",
                "barcode": patron["Barcode"],
                "active": True,
                "patronGroup": patron_group,
                "departments": [],
                "personal":
                    {
                        "lastName": patron["Last_Name"],
                        "firstName": patron["First_Name"],
                        "middleName": '',
                        "email": email,
                        "phone": '',
                        "preferredContactTypeId": "Email"
                },
                "expirationDate": expiration_date,
                "customFields": {
                    "institution": "UMass Amherst",
                    "user type": "Special Programs"
                }
            }

            self.patron_out.append(patron_json)

        logging.info('%s Patron Records Converted', len(self.patron_out))
        self._logElapsedTime()
        logging.info("Patron Records converted successfully\n")

    # Saves Patron data together in a json file that is ready-to-load
    def saveLoadData(self):
        with open(self.patron_out_file_name, 'w', encoding='utf-8') as outfile:
            for patron in self.patron_out:
                print(patron)
                outfile.write(f"{json.dumps(patron)}\n")
        logging.info('Patron Records saved to: %s', self.patron_out_file_name)


if __name__ == "__main__":
    config = '.env'
    dotenv.load_dotenv(config)
    start_time = datetime.now()
    logFile = f'{os.getenv("logFileDirectory")}/{start_time.year}-{start_time.month}-{start_time.day}--{start_time.hour}-{start_time.minute}-{start_time.second}.log'
    logging.basicConfig(filename=logFile, encoding='utf-8', level=logging.DEBUG,
                        format='%(asctime)s | %(levelname)s | %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
    print(f"Saving log to: {logFile}")
    logging.info('Beginning Log')

    # Logs Current Configuration and Raises an exception if a required configuration field is missing
    for field in ['patronFileName', 'destinationFolder', 'logFileDirectory']:
        try:
            logging.info('Config - %s = %s', field, os.getenv(field))
        except ValueError as exc:
            logging.critical('.env file must contain a value for %s', field)
            raise ValueError from exc

    # Actually uses the object to convert data
    converter = PatronDataTransformer(config, start_time)
    converter.transformPatronRecords()
    converter.saveLoadData()
