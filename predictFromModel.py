import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation


class prediction:

    def __init__(self,path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.pred_data_val = Prediction_Data_validation(path)

    def predictionFromModel(self):
        """
        Method Name: predictionFromModel
        Description: This method do the prediction on new data
        Output: A csv file with new data and prediction
        On Failure: Raise Exception

        Version: 1.0
        """

        try:
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.log_writer.log(self.file_object,'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data=data_getter.get_data() # getting the data

            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer) # object initialization.
            data = preprocessor.scaleData(data) # scaling the data

            #data = preprocessor.enocdeCategoricalvalues(data)

            file_loader=file_methods.File_Operation(self.file_object,self.log_writer) # object initialization.
            kmeans=file_loader.load_model('KMeans') # loading the model file from directory

            ##Code changed
            clusters=kmeans.predict(data) # drops the first column for cluster prediction
            data['clusters']=clusters
            clusters = data['clusters'].unique()
            result =[]
            for i in clusters:
                cluster_data= data[data['clusters']==i] # getting the data for the cluster
                cluster_data = cluster_data.drop(['clusters'],axis=1) # drops the cluster column
                model_name = file_loader.find_correct_model_file(i) # finding the correct model file for the cluster
                model = file_loader.load_model(model_name) # loading the model file from directory
                for val in (model.predict(cluster_data)):  # predicting the cluster
                    # Encoding the output numerical to original output label: forest type name.
                    if val ==0:
                        result.append("Lodgepole_Pine")
                    elif val==1:
                        result.append("Spruce_Fir")
                    elif val==2:
                        result.append("Douglas_fir")
                    elif val==3:
                        result.append("Krummholz")
                    elif val==4:
                        result.append("Ponderosa_Pine")
                    elif val==5:
                        result.append("Aspen")
                    elif val==6:
                        result.append("Cottonwood_Willow")
            result = pandas.DataFrame(result, columns=['Predictions']) # converting the result into dataframe
            path="Prediction_Output_File/Predictions.csv" # path to save the output file
            result.to_csv("Prediction_Output_File/Predictions.csv",header=True,mode='a+') #appends result to prediction file
            self.log_writer.log(self.file_object,'End of Prediction')

        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path





