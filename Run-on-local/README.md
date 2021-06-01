This folder contains the application code and the airflow DAG file which will orchestrate the application.
Prerequisite :
* We must have SPARK_HOME environment ready
* Airflow cluster running and its UI is appearing as it should be.

Also in the Airflow DAG file line number 58, it needs an external input which we are getting from the variable in the UI.
Screenshot attached:
![Screenshot 2021-06-01 at 01 29 08](https://user-images.githubusercontent.com/17614336/120251257-e93ec080-c278-11eb-8bb5-a92283f638ad.png)


I have used spark, keeping in mind of the scalability. The current code can be reused for big dataset. In that case Cluster size, cluster configuration,  along with the Spark-Submit parameters in the DAG file to be changed according to the size of the input data)
In addition to these, you must change the input and out put location of the application which will be used for taking input files and resultant output dataset.

Further Improvement:
Based on the time limitation, I have hard coded stuffs in the application and DAG files. This can be a point pf improvement.
