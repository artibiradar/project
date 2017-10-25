#6) Find the percentage and the count of each case status on total applications for each year. Create a graph depicting the pattern of All the cases over the period of time

register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
data = LOAD '/user/hive/warehouse/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);
number= filter data by $1 is not null and $1!='NA';
temp= group number by $7;
total= foreach temp generate group,COUNT(number.$0);
dump total;

number1= filter data by $7 is not null and $7!='NA';
temp1= group number1 by ($7,$1);
yearsoccount= foreach temp1 generate group,group.$0,COUNT($1);
dump yearsoccount;

joined= join yearsoccount by $1,total by $0;
ans= foreach joined generate FLATTEN($0),(long)($2*100)/$4,$2;
dump ans;

