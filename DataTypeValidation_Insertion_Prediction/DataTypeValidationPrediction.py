import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from application_logging.logger import App_Logger


class dBOperation:
    """
          This class shall be used for handling all the SQL operations.
          Version: 1.0
          Revisions: None

          """

    def __init__(self):
        self.path = 'Prediction_Database/'
        self.badFilePath = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()


    def dataBaseConnection(self,DatabaseName):

        """
            Method Name: dataBaseConnection
            Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
            Output: Connection to the DB
            On Failure: Raise ConnectionError

            Version: 1.0
            Revisions: None

        """
        try:
            conn = sqlite3.connect(self.path+DatabaseName+'.db')

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createTableDb(self,DatabaseName,column_names):

        """
           Method Name: createTableDb
           Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
           Output: None
           On Failure: Raise Exception

           Version: 1.0
           Revisions: None

        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

            conn.close()

            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Tables created successfully!!")
            file.close()

            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e


    def insertIntoTableGoodData(self,Database):

        """
            Method Name: insertIntoTableGoodData
            Description: This method inserts the Good data files from the Good_Raw folder into the
                        above created table.
            Output: None
            On Failure: Raise Exception

            Version: 1.0
            Revisions: None

                """

        conn = self.dataBaseConnection(Database) # connect to database
        goodFilePath= self.goodFilePath # set the source path where the files are located
        badFilePath = self.badFilePath # set the destination path to store the files that are moved to bad
        onlyfiles = [f for f in listdir(goodFilePath)] # get all files from the source directory
        log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+') # create log file

        for file in onlyfiles: # iterate over files
            try:
                with open(goodFilePath+'/'+file, "r") as f: # open file from source directory
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                # read the data line by line and insert into table
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_))) # 
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit() # commit the changes
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback() # rollback changes in case of any error
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
                raise e

        conn.close() # close database connection
        log_file.close() # close log file


    def selectingDatafromtableintocsv(self,Database):

        """
            Method Name: selectingDatafromtableintocsv
            Description: This method exports the data in GoodData table as a CSV file. in a given location.
                         above created.
            Output: None
            On Failure: Raise Exception

            Version: 1.0
            Revisions: None

                """

        self.fileFromDb = 'Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Prediction_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.dataBaseConnection(Database) # connect to database
            sqlSelect = "SELECT *  FROM Good_Raw_Data" # query to fetch all the rows from table.
            cursor = conn.cursor() # get cursor object to execute query

            cursor.execute(sqlSelect) # fetch all the rows from table using query

            results = cursor.fetchall()

            #Get the headers of the csv file
            headers = [i[0] for i in cursor.description] # fetch headers 

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            raise e





