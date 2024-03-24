import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from sklearn.model_selection import train_test_split
from dataclasses import dataclass
from db_connector import get_db_engine

@dataclass
class DataIngestionConfig :
    train_data_path : str = os.path.join('artifacts','train.csv')
    test_data_path : str = os.path.join('artifacts','test.csv')
    raw_data_path : str = os.path.join('artifacts','raw_data.csv')

class DataIngestion :
    def __init__(self) :
        self.ingestion_config = DataIngestionConfig()
    
    def initiate_data_ingestion(self) :
        logging.info("Entered the data ingestion component")
        try :
            engine = get_db_engine()
            logging.info('Established connection with MySQL Database.')
            df = pd.read_sql('SELECT * FROM student_info', con=engine)
            logging.info('Read the dataset from the MySQL Database.')

            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path),exist_ok=True)
            df.to_csv(self.ingestion_config.raw_data_path,index=False,header=True)
            
            logging.info('Train test split initiated')
            train_set, test_set = train_test_split(df,test_size=0.2,random_state=42)
            train_set.to_csv(self.ingestion_config.train_data_path, index = False, header = True)
            test_set.to_csv(self.ingestion_config.test_data_path, index = False, header = True)
            
            logging.info('Ingestion of the data is completed.')
            engine.dispose()
            logging.info('Database connection closed.')
            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        except Exception as e :
            logging.error(f"An error occurred during data ingestion: {e}")
            raise CustomException(e,sys)
        
if __name__=="__main__" :
    obj = DataIngestion()
    obj.initiate_data_ingestion()