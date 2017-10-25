#11) Export result for question no 10 to MySql database

1.mysql –u root –p
Password$:hduser

2.hadoop fs -mkdir /pig/question10

3. hadoop fs -put /home/pig/question10.pig /project/question10

4.create database h1b; 

5.use h1b;

6.create table question11 (job_title varchar(200) ,success_rate  float, petitions int) ;

7. describe question11;

8.Sqoop export –connect jdbc:mysql://localhost/h1b –username root  –password hduser –export-dir/project/question101 –input-fields-terminated-by ’\t’;
