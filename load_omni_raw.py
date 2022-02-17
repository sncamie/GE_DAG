from utils import * 

param_path = "/omni/staging/databases/mahle_behr"
omni= connect_omni_db(parameter_store_path=param_path)

def load_omni_raw():
    
    model_query = """
    select * from data_store_rawvalue where parameter_id =UUID('78f330e9-8900-45e4-aa76-1bc412d4d65d') or
    parameter_id =UUID('d52bbcba-f1f6-4885-8510-dbaa6f91a4e0')
    or parameter_id =UUID('fcd34841-d2b5-46d5-bacc-0a934412b491')
    or parameter_id =UUID('29fa6a90-ca21-40ef-8a72-f7035aa69226')
    and timestamp >=now()-interval '20 minutes';;

    """
    with omni.conn as cur:
        result = cur.execute(model_query).fetchall()

    df =pd.DataFrame(result)

    df.columns=['id','value', 'timestamp','origin_data_source_id','parameter_id', 'grouping_key']


    df2=df.pivot(index='timestamp', values='value', columns='parameter_id')

    df2.columns = ['CalibrationDriveSpeedPercent','SpeedDifferencePercent','FormingDriveSpeedPercent', 'WeldCurrent']

    df2.to_csv('/home/ncamiso.khanyile/Data/data_store_rawvalue/latestfile.csv')


def load_bobs():

    model_query =  """
    select * from bob_bobstats where parameter_id = '78f330e9-8900-45e4-aa76-1bc412d4d65d' or 
    parameter_id = 'd52bbcba-f1f6-4885-8510-dbaa6f91a4e0'
    or parameter_id='29fa6a90-ca21-40ef-8a72-f7035aa69226' 
    or parameter_id = 'fcd34841-d2b5-46d5-bacc-0a934412b491';

    """
    with omni.conn as cur:
        result = cur.execute(model_query).fetchall()

    df =pd.DataFrame(result)
    df.columns=['id','mean', 'median','bob_lower','bob_upper', 'created_at', 'updated_at', 'bob_id', 'parameter_id']

    df.to_csv('data.csv')


    df2=pd.read_csv('data.csv')


    df2=df2.replace(to_replace=['29fa6a90-ca21-40ef-8a72-f7035aa69226','d52bbcba-f1f6-4885-8510-dbaa6f91a4e0','fcd34841-d2b5-46d5-bacc-0a934412b491','78f330e9-8900-45e4-aa76-1bc412d4d65d'],
    value=['CalibrationDriveSpeedPercent','FormingDriveSpeedPercent' ,'WeldCurrent','SpeedDifferencePercent'],
    )

    df3=df2.pivot(index='created_at', values=['median','bob_lower','bob_upper'], columns='parameter_id')


    d= df3.columns.swaplevel().map('_'.join)

    df3.columns=df3.columns.droplevel(0)

    df3.columns = d


    df3.to_csv('/home/ncamiso.khanyile/Data/bob_bobstats/bobs.csv')
