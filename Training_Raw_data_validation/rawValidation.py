import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger





class Raw_Data_validation:

    """
    This class shall be used for handling all the validation done on the Raw Training Data!!.

    Version: 1.0
    Revisions: None

    """

    def __init__(self,path):
        self.Batch_Directory = path # Get the directory where the data files are located
        self.schema_path = 'schema_training.json' # path where the schema is stored
        self.logger = App_Logger() # Create instance of Application Logger
 

    def valuesFromSchema(self):
        """
            Method Name: valuesFromSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            On Failure: Raise ValueError,KeyError,Exception

            Version: 1.0
            Revisions: None

            """
        try:
            with open(self.schema_path, 'r') as f: # Open file in read mode
                dic = json.load(f) # Load the json file in python dictionary format
                f.close()

            # Getting paramete form the dictionary
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            # Open lof file in append mode
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()



        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
        Method Name: manualRegexCreation
        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                    This Regex is used to validate the filename of the training data.
        Output: Regex pattern
        On Failure: None

        Version: 1.0
        Revisions: None

        """
        regex = "['forest_cover']+['\_'']+[\d_]+[\d]+\.csv" # Regex for extracting the fields from the file name
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
        Method Name: createDirectoryForGoodBadRawData
        Description: This method creates directories to store the Good Data and Bad Data
                    after validating the training data.

        Output: None
        On Failure: OSError

        Version: 1.0
        Revisions: None

                """

        try:
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/") # Joining the directory path and creating the directory
            if not os.path.isdir(path): # If a directory is not present, then only create the directory
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/") # Joining the directory path and creating the directory
            if not os.path.isdir(path):  # If a directory is not present, then only create the directory
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):

        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data
                            after loading the data in the table. Once the good files are
                            loaded in the DB,deleting the directory ensures space optimization.
            Output: None
            On Failure: OSError
    
            Version: 1.0
            Revisions: None

            """

        try:
            path = 'Training_Raw_files_validated/' # Directory to delete
            if os.path.isdir(path + 'Good_Raw/'): # Checking if directory exists
                shutil.rmtree(path + 'Good_Raw/') # Deleting the directory
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
            Method Name: deleteExistingBadDataTrainingFolder
            Description: This method deletes the directory made to store the bad Data.
            Output: None
            On Failure: OSError

            Version: 1.0
            Revisions: None

        """

        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        """
            Method Name: moveBadFilesToArchiveBad
            Description: This method deletes the directory made  to store the Bad Data
                            after moving the data in an archive folder. We archive the bad
                            files to send them back to the client for invalid data issue.
            Output: None
            On Failure: OSError

            Version: 1.0
            Revisions: None

        """
        now = datetime.now() # Current date and time
        date = now.date() # Current date
        time = now.strftime("%H%M%S") # Current time
        try: 

            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source): # Checking if source directory exists
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path): # Checking if path already exists
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest): # If path does not exist in archive, then make that path and move the files
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the training csv files as per given name in the schema!
                            Regex pattern is used to do the validation.If name format do not match the file is moved
                            to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

            Version: 1.0
            Revisions: None

        """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder() # Deleting Bad Data Folder if exists
        self.deleteExistingGoodDataTrainingFolder() # Deleting Good Data Folder if exists
        #create new directories
        self.createDirectoryForGoodBadRawData() # Create directory for good and bad data
        onlyfiles = [f for f in listdir(self.Batch_Directory)] # getting list of file names from the directory
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+') # Open a text file in append mode
            for filename in onlyfiles: # iterating over files in the directory
                if (re.match(regex, filename)): # This condition matches the regex pattern in the filename
                    splitAtDot = re.split('.csv', filename) # split the file name by dot
                    splitAtDot = (re.split('_', splitAtDot[0])) # split the first part by underscore
                    if len(splitAtDot[2]) == LengthOfDateStampInFile: # check if length of the date portion is same as in the schema
                        if len(splitAtDot[3]) == LengthOfTimeStampInFile: # check if length of the time portion is same as in the schema
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw") # Copy the file to the Good Raw Data folder
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
            Method Name: validateColumnLength
            Description: This function validates the number of columns in the csv files.
                        It is should be same as given in the schema file.
                        If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                        If the column number matches, file is kept in Good Raw Data for processing.
                        The csv file is missing the first column name, this function changes the missing name to "Wafer".
            Output: None
            On Failure: Exception

            Version: 1.0
            Revisions: None

        """
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+') # Open a text file in append mode
            self.logger.log(f,"Column Length Validation Started!!") 
            for file in listdir('Training_Raw_files_validated/Good_Raw/'): # going through all files in the good raw folder
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns: # check if number of columns match in schema
                    pass # the file is already on Good Raw Data folder, no action required.
                else:
                    # move the bad files to bad data folder
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
            Method Name: validateMissingValuesInWholeColumn
            Description: This function validates if any column in the csv file has all values missing.
                        If all the values are missing, the file is not suitable for processing.
                        SUch files are moved to bad raw data.
            Output: None
            On Failure: Exception

            Version: 1.0
            Revisions: None

            """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')  # Open a text file in append mode
            self.logger.log(f,"Missing Values Validation Started!!")

            for file in listdir('Training_Raw_files_validated/Good_Raw/'): # going through all files in the good raw folder
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                count = 0 # a count to check if all values are null
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]): # check if all values in column are null
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()












