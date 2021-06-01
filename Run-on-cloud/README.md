This text provides a short description to run the application on AWS cloud .
The idea is, we will have an airflow cluster running on a EC2 instance. The DAG on the airflow cluster will run a pyhton code which will steps on EMR cluster. 
Downloading the application code from AWS s3, run on EMR and write the resultant dataset to AWS S3 etc.

In order to run this section of the code,
* Get airflow running on EC2.
* Get a EMR cluster running. 

Feed the EMR cluster id: 
![Screenshot 2021-06-01 at 01 51 06](https://user-images.githubusercontent.com/17614336/120252248-df6a8c80-c27b-11eb-8785-e94ceafec361.png)

