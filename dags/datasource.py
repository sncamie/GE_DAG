from utils import *


#Predifined variables for each data source, suite, checkpoint,etc
datasource_name = "RSM9"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"

prefix = "rsm9_iba_pda/"
bucket_name = "omni-upload-mahle" 
group_names = ["myfiles"]
regex_pattern = "(.*)\.parquet"
reader_method = "read_parquet"
data_asset_name = "RSM9"



#Datasource configuration

datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {            
	        "class_name": "RuntimeDataConnector",            
	        "batch_identifiers": ["default_identifier_name"],     
    },
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetS3DataConnector",
            "bucket": bucket_name,
            "prefix": prefix,
            "assets":{
                data_asset_name:{
                    "group_names":group_names,
                    "pattern":regex_pattern,
                },

            },

            
        },
    },
}
#calling a batch of data from our data source, using index -1 to get the latest file. Can be any python slice
batch_request = BatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_configured_data_connector_name",
    data_asset_name=data_asset_name,
    data_connector_query= {"index":-1},
    batch_spec_passthrough={
        "reader_method": reader_method,
     
    },
)

#conifgure checkpoint 

checkpoint_config = f"""
name: {checkpoint_name}
config_version: 1
class_name: Checkpoint
validations:
- batch_request: {batch_request}
  expectation_suite_name: {expectation_suite_name}
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
""" 


def connect_to_datasource():
    """Connects to raw data in the filesystem/raw directory, and adds
    configuration to YAML file if successful.
    """

    try:
        # connect to raw data
        context.test_yaml_config(yaml.dump(datasource_config))

        # empty Expectation Test Suite, its only purpose is to validate the Datasource connection
        context.create_expectation_suite(
            expectation_suite_name="test_suite", overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite"
        )
        print(validator.head())

    except IndexError as ex:  
        logging.exception(
            f"""Unmatched data references are not available for connection.\
            Ensure that your base directory: "{prefix }", group names "{prefix}",\
            and regex pattern "{regex_pattern}" are correct.
        """
        )

    except Exception as ex:
        logging.exception(
            f'Cannot connect to file in base directory "{prefix}"'
        )

    else:
        context.add_datasource(**datasource_config)
        logging.info("Added raw data file config to `great_expectations.yml` file")



def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )


    #Table level expectations 
    validator.expect_table_columns_to_match_ordered_list(
        column_list=[
            "timestamp",
            "bsp/B102NearlyOnTop",
            "bsp/B112OnTop1",
            "bsp/B123WeightUp1",
            "bsp/B133NearlyOnTop",
            "bsp/B143OnTop2",
            "bsp/B154WeightUp2",
            "bsp/B164NearlyOnTop",
            "bsp/B174OnTop3",
            "bsp/B185DownWeightUp1",
            "bsp/B195NearlyOnTop",
            "bsp/B205OnTop4",
            "bsp/B216WeightUp3",
            "bsp/B226NearlyOnTop",
            "bsp/B236OnTop5",
            "bsp/B27LoopBottomTubeForming",
            "bsp/B28coilarmintoppos",
            "bsp/B28LoopTopTubeForming",
            "bsp/B3LoopDown",
            "bsp/B4LoopTop",
            "bsp/B5StripInMachine",
            "bsp/B61DownWeightUp",
            "bsp/B71NearlyOnTop",
            "bsp/B81OnTop",
            "bsp/B92WeightUp",
            "bsp/CurrentCoilDiameter",
            "bsp/DecoilerDirection",
            "bsp/FactorTower1",
            "bsp/FactorTower2",
            "bsp/FactorTower3",
            "bsp/FactorTower4",
            "bsp/FactorTower5",
            "bsp/FactorTower6",
            "bsp/FillingSpeedNew",
            "bsp/FillingSpeedOld",
            "bsp/JogSpeed",
            "bsp/JogSpeedNew",
            "bsp/JogSpeedOld",
            "bsp/LargestCoilDiameter",
            "bsp/MillSpeed",
            "bsp/PullingAfterACCFmax",
            "bsp/PullingAfterACCFmin",
            "bsp/PullingBeforeAccFmax",
            "bsp/PullingBeforeACCFmin",
            "bsp/Pull1",
            "bsp/Pull2",
            "bsp/Pulling2AfterACCSpeedNew",
            "bsp/Pulling2AfterACCSpeedOld",
            "bsp/Roller1",
            "bsp/Roller2",
            "bsp/Roller3",
            "bsp/Roller4",
            "bsp/Roller5",
            "bsp/Roller6",
            "bsp/SmallestCoilDiameter",
            "bsp/SpeedToDriveAcc1",
            "bsp/SpeedToDriveAcc2",
            "bsp/SpeedToDriveAcc3",
            "bsp/SpeedToDriveAcc4",
            "bsp/SpeedToDriveAcc5",
            "bsp/SpeedToDriveAcc6",
            "bsp/SpeedToDrivePullOut1",
            "bsp/SpeedToDrivePullOut2",
            "bsp/TopPosSpeedDiffNew",
            "bsp/TopPosSpeedDiffOld",
            "bsp/Tower1Fmax",
            "bsp/Tower1Fmin",
            "bsp/Tower2Fmax",
            "bsp/Tower2Fmin",
            "bsp/Tower3Fmax",
            "bsp/Tower3Fmin",
            "bsp/Tower4Fmax",
            "bsp/Tower4Fmin",
            "bsp/Tower5Fmin",
            "bsp/Tower6Fmax",
            "bsp/Tower6Fmin",
            "bsp/CoilDiameter2",
            "bsp/CoilDiameter1",
            "bsp/Decoiler1Active",
            "bsp/Decoiler2Active",
            "bsp/Drehrichtungsumkehr",
            "bsp/LoopControl",
            "bsp/UsedATag120u10",
            "bsp/UsedATag120u11",
            "bsp/UsedATag120u3",
            "bsp/UsedATag120u4",
            "bsp/UsedATag120u6",
            "bsp/UsedATag120u7",
            "bsp/UsedATag120u8",
            "bsp/UsedATag120u9",
            "cutterplc/CutSignal",
            "cutterplc/EnableCutGoodTubes",
            "cutterplc/EnableCutScrapTubes",
            "cutterplc/LubePulseTime",
            "cutterplc/LubeWaitingTime",
            "cutterplc/MachineSpeed",
            "cutterplc/Y01OilForGears",
            "cutterplc/Y02OilForGuiding",
            "cutterplc/Y03OilForCutterDieL",
            "cutterplc/YP1AirForGuiding",
            "cutterplc/YP4AirForMask",
            "cutterplc/YP8AirForCutterDieL",
            "cutterplc/DCCurrentLimitGenerator",
            "cutterplc/HFTransfoPrimaryThermo",
            "cutterplc/HFTransfoSecondThermo",
            "cutterplc/LoadResistProtect",
            "cutterplc/Neg15VccGr5Percentage",
            "cutterplc/Neg15VccLess5Percentage",
            "emmediplc/OilThermostatBuchholz",
            "emmediplc/Plus15VccGr5Percentage",
            "emmediplc/Plus15VccLess5Percentage",
            "emmediplc/RecipeValueForTubeHeight",
            "emmediplc/RecipeValueForTubeWidth",
            "emmediplc/SCRPulseSuppress",
            "emmediplc/SCRThermostat",
            "emmediplc/STAB1Protect",
            "emmediplc/WeldCurrent",
            "emmediplc/WeldVoltage",
            "emmediplc/ACCurrentLimitGenerator",
            "emmediplc/AnodeOverloadRelay",
            "emmediplc/DCVoltageLimitGenerator",
            "emmediplc/DistilledWaterThermo",
            "emmediplc/FeedbackTransFlowSwitch",
            "emmediplc/FilamentAirPressure",
            "emmediplc/FilamentHeatingOK",
            "emmediplc/filterInductancethermo",
            "emmediplc/GridOverloadRelay",
            "emmediplc/HFPrimaryFlowSitch",
            "emmediplc/HFSecondFlowSwitch",
            "emmediplc/HVCabinetThermostat",
            "emmediplc/ServiceJumper",
            "emmediplc/TankCapacitorSwitch",
            "emmediplc/TriodeFilamentHeating",
            "emmediplc/TriodeFlowswitch",
            "emmediplc/TriodePressureSwitch",
            "emmediplc/Triodethermostat",
            "emmediplc/WeldingCoilPressureSwitch",
            "emmediplc/WeldPalletOnline",
            "mainplc/AccStripEnd",
            "mainplc/AccReady",
            "mainplc/ActualWeldPalletDrive",
            "mainplc/AirPressureOK",
            "mainplc/AirPressureOK1",
            "mainplc/AutoOn",
            "mainplc/AutoSelected",
            "mainplc/BehrRollsSetActive",
            "mainplc/BucketConveyorTurningLeft",
            "mainplc/BucketConveyorTurningRight",
            "mainplc/CalibrationCurrent",
            "mainplc/CalibrationSpeed",
            "mainplc/CycleBandOn",
            "mainplc/CalibrationDriveSpeed",
            "mainplc/CalibrationDriveSpeedPercent",
            "mainplc/CalibrationMotorRunning",
            "mainplc/ChippingOn",
            "mainplc/CoolingBoxBottomLubFlowMLMin",
            "mainplc/CoolingBoxTopLubFlowMLMin",
            "mainplc/CutterBridgedForStart",
            "mainplc/CutterControlOn",
            "mainplc/CutterEnabledForGoodAndBadTubes",
            "mainplc/CutterEnabledForGoodTubes",
            "mainplc/CutterInLineLaserOut",
            "mainplc/CutterJobBit",
            "mainplc/CutterLengthTol2",
            "mainplc/CutterOK",
            "mainplc/CutterScrapFlapStatus",
            "mainplc/CutterWeldSeamSinking",
            "mainplc/ESActivated",
            "mainplc/EddyCheck90ScrapCount",
            "mainplc/EddyCheckAbsoluteScrap",
            "mainplc/EddyCheckOn",
            "mainplc/EddyCheckTooLowScrapCount",
            "mainplc/FormCurrent",
            "mainplc/FormSpeed",
            "mainplc/FaultDetectedOnCentralTube",
            "mainplc/FeltRollerBotLubFlowMLMin",
            "mainplc/FeltRollerTopLubFlowMLMin",
            "mainplc/FlatbandLongShortOn",
            "mainplc/FormingDriveSpeed",
            "mainplc/FormingDriveSpeedPercent",
            "mainplc/FormingMotorRunning",
            "mainplc/ForwardSpeedToEmmedi",
            "mainplc/GeneratorReady",
            "mainplc/InLineLaserMeasurement1Cc",
            "mainplc/InLineLaserMeasurement3Aa",
            "mainplc/InLineLaserMeasurement4Width",
            "mainplc/InLineLaserMeasurement2bb1",
            "mainplc/InLineLaserMeasurement5bb2",
            "mainplc/KellerOK",
            "mainplc/KellerOn",
            "mainplc/KellerRunning",
            "mainplc/MachineInAutoRun",
            "mainplc/MillSpeed",
            "mainplc/MotorProtectFans",
            "mainplc/OffsetPercentage",
            "mainplc/PillzRelayPnozOK",
            "mainplc/ProductionRunningWithCut",
            "mainplc/PutCentralTubeCoolingHigh",
            "mainplc/PutCoolingPumpOn",
            "mainplc/PutScrapingStationBlowAwayOn",
            "mainplc/PutWaterFlowWeldingRolls",
            "mainplc/QStopGreen",
            "mainplc/QStopRed",
            "mainplc/QStopYellow",
            "mainplc/RSMSpeedInmmPerSec",
            "mainplc/RejectBadTubesAtFlatBand",
            "mainplc/RejectComandFromInlineLaser",
            "mainplc/RejectCommandDueToCutter",
            "mainplc/RejectorInAuto",
            "mainplc/ResetScrapCounter",
            "mainplc/RollSet1Behr0Schoeler",
            "mainplc/SchoelerRollSetActive",
            "mainplc/SpeedDifferencePercent",
            "mainplc/StampingRollOn",
            "mainplc/StartConveyorsInAuto",
            "mainplc/StripLubOK",
            "mainplc/TotalScrap",
            "mainplc/WaterFlowWeldingCoil1",
            "mainplc/WeldDrivePalletPotValue",
            "mainplc/WeldPalletActualSpeedMps",
            "mainplc/WeldPalletActualSpeedPercentage",
            "mainplc/WeldPalletGearRatio",
            "mainplc/WeldPalletMaxSpeedOfRolls",
            "mainplc/WeldPalletNotOnMachine",
            "mainplc/WeldPalletOnMachine",
            "mainplc/WeldPalletOutlineOfRoll",
            "mainplc/WeldPalletOutsideDiam",
            "mainplc/WeldPalletSpeed",
            "mainplc/WeldPalletActive",
            "mainplc/WeldSpeedOffsetMaxSpeed",
            "mainplc/WeldingOffCommand",
            "mainplc/WeldingOnCommand",
            "mainplc/EddyCheckOutput1",
            "mainplc/EddyCheckOutput2",
            "mainplc/EddyCheckOutput3",
            "mainplc/EddyCheckOutput4",
            "mainplc/EddyCheckSystemReady",
            "mainplc/LoopSmallRSMInlet",
            "mainplc/LubrTankIsEmpty",
            "mainplc/TankOfTubeCoolingFull",
            "mainplc/TankOfTubeCoolingOverful",
            "mainplc/WaterFlowWeldingArm",
            "mainplc/WaterFlowWeldingRolls",
            "rejectplc/SignalSchoeler",
            "rejectplc/RejectSignal",
            "rejectplc/TemperatureMotor1",
            "rejectplc/TemperatureMotor2",
            "mainplc/RSMSpeedInMPerSec",
            "mainplc/RollSetSelected",
            "mainplc/TubeLengthJob1",
            "mainplc/TubeLengthJob2",
            "bsp/CoilDiameter1_Scaled",
            "bsp/CoilDiameter2_Scaled",
            "bsp/LoopControl_Scaled",
            "bsp/Pull1_Scaled",
            "bsp/Pull2_Scaled",
            "bsp/Roller1_Scaled",
            "bsp/Roller2_Scaled",
            "bsp/Roller3_Scaled",
            "bsp/Roller4_Scaled",
            "bsp/Roller5_Scaled",
            "bsp/Roller6_Scaled",
            "bsp/CalibrationCurrent_Scaled",
            "bsp/CalibrationDriveSpeedPercent_Scaled",
            "bsp/CalibrationSpeed_Scaled",
            "bsp/FormCurrent_Scaled",
            "bsp/FormSpeed_Scaled",
            "bsp/ForwardSpeedToEmmedi_Scaled",
            "bsp/SpeedToDrivePull1_Scaled",
            "bsp/SpeedToDrivePull2_Scaled",
            "bsp/Tower5Fmax",
            "mainplc/WeldVoltage_Scaled",
            "mainplc/WeldCurrent_Scaled",
        ]
    )


    #Column level expectations 

    #bsp/CalibrationDriveSpeedPercent_Scaled

    validator.expect_column_values_to_be_between(
        column= "bsp/CalibrationDriveSpeedPercent_Scaled",
        min_value=0.0,
        max_value=32.7,
    )

    validator.expect_column_values_to_not_be_null(column="bsp/CalibrationDriveSpeedPercent_Scaled")

    #mainplc/CalibrationDriveSpeedPercent

    validator.expect_column_values_to_be_between(
        column= "mainplc/CalibrationDriveSpeedPercent",
        min_value=0.0,
        max_value=35.6,
    )

    validator.expect_column_values_to_not_be_null(
        column="mainplc/CalibrationDriveSpeedPercent"
    )

    

    #mainplc/FormingDriveSpeedPercent
    validator.expect_column_values_to_be_between(
        column= "mainplc/FormingDriveSpeedPercent",
        min_value=0.0,
        max_value=36.0,
    )

    validator.expect_column_values_to_not_be_null(column="mainplc/FormingDriveSpeedPercent")


    # mainplc/SpeedDifferencePercent
    validator.expect_column_values_to_be_between(
        column= "mainplc/SpeedDifferencePercent",
        min_value=0.0,
        max_value=4.0,
    )

    validator.expect_column_values_to_not_be_null(column="mainplc/SpeedDifferencePercent")

    #mainplc/WeldCurrent_Scaled
    validator.expect_column_values_to_be_between(
        column= "mainplc/WeldCurrent_Scaled",
        min_value=0.0,
        max_value=2.8,
    )

    validator.expect_column_values_to_not_be_null(column="mainplc/WeldCurrent_Scaled")

    validator.save_expectation_suite(discard_failed_expectations=False)


def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))
    result = context.run_checkpoint(checkpoint_name)
    print(f'Successful checkpoint validation: {result["success"]}\n')




connect_to_datasource()
create_expectation_suite(expectation_suite_name=expectation_suite_name)
create_checkpoint(checkpoint_name=checkpoint_name)