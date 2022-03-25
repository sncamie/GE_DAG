from utils import * 

def load_omni_raw():
    param_path = "/omni/staging/databases/mahle_behr"
    omni= connect_omni_db(parameter_store_path=param_path)
    model_query = """
    select * from data_store_rawvalue where parameter_id =UUID('78f330e9-8900-45e4-aa76-1bc412d4d65d') or
    parameter_id =UUID('d52bbcba-f1f6-4885-8510-dbaa6f91a4e0')
    or parameter_id =UUID('fcd34841-d2b5-46d5-bacc-0a934412b491')
    or parameter_id =UUID('29fa6a90-ca21-40ef-8a72-f7035aa69226');

    """
    with omni.conn as cur:
        result = cur.execute(model_query).fetchall()

    df =pd.DataFrame(result)

    df.columns=['id','value', 'timestamp','origin_data_source_id','parameter_id', 'grouping_key']


    df2=df.pivot(index='timestamp', values='value', columns='parameter_id')

    df2.columns = ['CalibrationDriveSpeedPercent','SpeedDifferencePercent','FormingDriveSpeedPercent', 'WeldCurrent']

    df2.to_csv('/home/ncamiso.khanyile/Data/data_store_rawvalue/latestfile.csv')