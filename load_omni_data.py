from utils import * 

param_path = "/omni/staging/databases/mahle_behr"
omni= connect_omni_db(parameter_store_path=param_path)
root_directory ='/home/ncamiso.khanyile/Data/'

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


def load_bobs(loc_path):

    """Downloads bob stats data from bob_bobstats table saved in 
    omniDB for four parameters of interest. 
    Unmelts the data and saves csv to a folder
    where the checkpoint will run. 
    """

    model_query = """
    select a.created_at, a.median, a.bob_lower,a.bob_upper, b.title 
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


    df3.to_csv(loc_path)


def load_prescribe_data(loc_path):

    """Downloads prescribe data from prescribe_prescribereportdata table saved in 
    omniDB for four parameters of interest. 
    Unmelts the data and saves csv to a folder
    where the checkpoint will run. 
    """

    model_query = """
    select a.id as id, a.bob_upper as bob_upper, a.bob_lower as bob_lower,a.bob_median as bob_median,a.bob_target as bob_target, b.title
    from prescribe_prescribereportdata as a
    left join parameter_parameter as b
    on a.parameter_id=b.id 
    where b.title='FormingDriveSpeedPercent' or 
    b.title='WeldCurrent' or
    b.title='CalibrationDriveSpeedPercent'
    or b.title= 'SpeedDifferencePercent';

    """


    with omni.conn as cur:
        result = cur.execute(model_query).fetchall()



    df =pd.DataFrame(result,columns=['id','bob_upper', 'bob_lower', 'bob_median','bob_target', 'parameter_id'])



    df3=df.pivot(index='id', values=['bob_upper', 'bob_lower', 'bob_median','bob_target'], columns='parameter_id')


    d= df3.columns.swaplevel().map('_'.join)
    df3.columns=df3.columns.droplevel(0)
    df3.columns=d
    df3.to_csv(loc_path)