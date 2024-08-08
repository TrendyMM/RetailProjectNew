
import pytest
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state,filter_orders_generic
from lib.ConfigReader import get_app_config

def test_read_customers_df(spark):
    customers_count = read_customers( spark ,"LOCAL").count()
    assert customers_count == 12435        # if true , test case will pass 

def test_read_orders_df(spark):
    orders_count = read_orders( spark ,"LOCAL").count()
    assert orders_count == 68884    

@pytest.mark.transformationTest
def test_filter_closed_orders_df(spark):
    orders_df = read_orders( spark ,"LOCAL")
    filtered_count = filter_closed_orders( orders_df).count()
    assert filtered_count == 7556

@pytest.mark.skip("work in progress")      # system defined marker 
def test_read_app_config(spark):
    config = get_app_config("LOCAL")       # this will be dictionary
    assert config["orders.file.path"] == "data/orders.csv"

@pytest.mark.slow()
def test_count_orders_state(spark, expected_results) : 
    customers_df = read_customers(spark,"LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect()  == expected_results.collect()

    # collect() will give the results in a local python list  
    # expected_results will also be a DF in memory , so use collect()

@pytest.mark.latest()
def test_check_closed_count (spark ): 
    orders_df = read_orders( spark ,"LOCAL")
    filtered_count = filter_orders_generic( orders_df,"CLOSED").count()
    assert filtered_count == 7556  

@pytest.mark.latest()
def test_check_pendingpayment_count (spark ): 
    orders_df = read_orders( spark ,"LOCAL")
    filtered_count = filter_orders_generic( orders_df,"PENDING_PAYMENT").count()
    assert filtered_count == 15030    

@pytest.mark.latest()
def test_check_complete_count (spark ): 
    orders_df = read_orders( spark ,"LOCAL")
    filtered_count = filter_orders_generic( orders_df,"COMPLETE").count()
    assert filtered_count == 22900       

@pytest.mark.parametrize (
        "status,count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22900)
         ]
)

@pytest.mark.generic_filter()
def test_check_count(spark,status,count) :
    orders_df = read_orders( spark ,"LOCAL")
    filtered_count = filter_orders_generic( orders_df,status).count()
    assert filtered_count == count  