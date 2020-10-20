
#### Setup
Linux comes with its own distribution of mongodb.
This is where the SparkJob writes the results. The mongodb server must be running locally before running the task.
Run the command below in shell to start the server:
```
mongod
```
If mongod is not installed. It should be setup, but that is out of the scope for this task for now.

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
