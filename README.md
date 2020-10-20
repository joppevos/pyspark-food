Reads data into Spark and calculate the net merchandise value of the order(ed products).

The net value is calculated as followed: 7% VAT applied to cold foods, 15% to hot foods and 9% on beverages.

After transforming the data is stored in MongoDB 

The output data is exposed via REST API. 

The projects showcases different skills. 
- Distributed computing (pyspark)
- Testing (pytest)
- result exposing on API (FastAPI)

#### Setup

There a single setup script in the repository. 
The scrip will setup the environment, submit the spark job and launch the API locally.
Run `bash setup.sh`.


Visit the displayed Uvicorn URL and request the spending's for user `5b6950c008c899c1a4caf2a1` 
```
http://127.0.0.1:8000/spend/5b6950c008c899c1a4caf2a1
----------------------------------------------------
{"customerId":"5b6950c008c899c1a4caf2a1","orders":11,"totalNetMerchandiseValueEur":88.11}
```
```
http://127.0.0.1:8000/spend/foo
----------------------------------------------------
"Unable to find customerId: foo"
```

#### Notes

All the packages used by Spark are compiled in `packages.zip`
To test the SparkSQL transformation there is a test to make the right assertions.
The API is served by FastAPI for its speed and easy deployment. FastAPI also provides automatic documentation.

#### Future improvements

- containerize 
