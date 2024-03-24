import pandas as pd
from sqlalchemy import create_engine


def get_db_engine() :
    engine = create_engine("mysql+mysqlconnector://root:ssgneb93@localhost:3306/student")
    df = pd.read_csv("data\\stud.csv")
    df.to_sql('student_info',engine,if_exists='replace',index=False)
    return engine