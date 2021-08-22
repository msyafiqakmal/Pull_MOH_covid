# Pull_MOH_covid

##Problem Statement
Extract covid data from MOH Daily for viz, without specifying which files. 

#solution
Using Git API, get the structure and using pandas pull all csv data, load postgres.
and to apply this as part of airflow. 

feel free to manually extract body for non-airflow deployment.

#missing file:
I exclude yaml file that store db credential. simply create dbconinfo.yaml file with the following details:-
  ```
  dbms: ""
  username: ""
  password: ""
  server: ""
  port: ""
  database: ""
  ```

  
