from utils import * 

param_path = "/omni/staging/databases/mahle_behr"
omni= connect_omni_db(parameter_store_path=param_path)
base_directory ='/home/ncamiso.khanyile/Data/'

def load_omni_raw(loc_path):

    """Downloads raw data from data_store_rawvalue table saved in 
    omniDB for four parameters of interest. 
    Unmelts the data and saves csv to a folder
    where the checkpoint will run. 
    """
    
    model_query = """
    select a.timestamp, a.value, b.title

    from data_store_rawvalue as a

    left join parameter_parameter as b
    on a.parameter_id=b.id 
    where b.title='FormingDriveSpeedPercent' or 
    b.title='WeldCurrent' or
    b.title='CalibrationDriveSpeedPercent'
    or b.title= 'SpeedDifferencePercent'
    and a.timestamp >=now()-interval '40 minutes';

    """

    with omni.conn as cur:
        result = cur.execute(model_query).fetchall()


    df =pd.DataFrame(result, columns=['timestamp', 'value', 'parameter_id'])


    df2=df.pivot(index='timestamp', values='value', columns='parameter_id')

    df2.to_csv(loc_path)


load_omni_raw(f'{base_directory}/data_store_rawvalue/latestfile2.csv')