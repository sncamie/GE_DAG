import pandas as pd
import time
import sys
sys.path.append("/home/ncamiso.khanyile/GE_DAG/Raw_files_dag/")


from utils import *

def getomnifiles(loc_path="/home/ncamiso.khanyile/Data/data_store_rawvalue/testdata.csv"):
    while True:
        model_query = """
        select a.id, a.median, a.bob_lower,a.bob_upper, b.title 
        from bob_bobstats as a
        left join parameter_parameter as b
        on a.parameter_id=b.id 
        where b.title='FormingDriveSpeedPercent' or 
        b.title='WeldCurrent' or
        b.title='CalibrationDriveSpeedPercent'
        or b.title= 'SpeedDifferencePercent';

        """


        with omni.conn as cur:
            result = cur.execute(model_query).fetchall()


        df =pd.DataFrame(result, columns=['id','median','bob_lower','bob_upper', 'parameter_id'])

        df3=df.pivot(index='id', values=['median','bob_lower','bob_upper'], columns='parameter_id')

        d= df3.columns.swaplevel().map('_'.join)

        df3.columns=df3.columns.droplevel(0)

        df3.columns = d


        
        yield df3.to_csv(loc_path)
        


getomnifiles()