import pytest
from lib.Utils import get_spark_session

@pytest.fixture             # this is where we write setup code , eg getting resources 
def spark():
    "creates spark session"      # docstring 
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()     # this is where we write teardown code , eg releasing resources

@pytest.fixture            
def expected_results(spark):
    "gives the expected results"      # docstring 
    results_schema = "state string , count int"
    return spark.read \
        .format ("csv") \
        .schema (results_schema) \
        .load ("data/test_result/state_aggregate.csv")

