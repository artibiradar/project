2 a) Which part of the US has the most Data Engineer jobs for each year?


register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
data1 = LOAD '/user/hive/warehouse/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);
job_title= filter data by $7=='2011';
a= group job_title by $4;
step_a= foreach a generate group,COUNT($1);
describe step_a;

job_title1= filter data1  by $7=='2012';
b= group job_title1 by $4;
step_b= foreach b generate group,COUNT($1);
describe step_b;

job_title2= filter data1  by $7=='2013';
c= group job_title2 by $4;
step_c= foreach c generate group,COUNT($1);
describe step_c;

job_title3= filter data1  by $7=='2014';
d= group job_title3 by $4;
step_d= foreach d generate group,COUNT($1);
describe step_d;

job_title4= filter data1  by $7=='2015';
e= group job_title4 by $4;
step_e= foreach e generate group,COUNT($1);
describe step_e;

job_title5= filter data1  by $7=='2016';
f= group job_title5 by $4;
step_f= foreach f generate group,COUNT($1);
describe step_f;

joined= join step_a by $0,step_b by $0,step_c by $0,step_d by $0,step_e by $0,step_f by $0;
describe joined;

year= foreach joined generate $0,$1,$3,$5,$7,$9,$11;
final = order year by $1 desc;
dump final;



