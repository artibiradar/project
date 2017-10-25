#9) Which are top ten employers who have the highest success rate in petitions?

register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
data = LOAD 'hdfs://localhost:54310/user/hive/warehouse/niit_h1b.db/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);
number= filter data by $1 is not null and $1!='NA';
petitions= group number by $2;
total= foreach petitions generate group,COUNT(number.$0);
dump total;

certified= filter data by $1 == 'CERTIFIED';
petitions1= group certified by $2;
totalcertified= foreach petitions1 generate group,COUNT(certified.$0);
dump totalcertified;

certified_with= filter data by $1 == 'CERTIFIED-WITHDRAWN';
petitions2= group certified_with by $2;
totalcertifiedwithdrawn= foreach petitions2 generate group,COUNT(certified_with.$0);
dump totalcertifiedwithdrawn;

joined= join totalcertified by $0,totalcertifiedwithdrawn by $0,total by $0;
joined= foreach joined generate $0,$1,$3,$5;
dump joined;

intermediateoutput= foreach joined generate $0,(float)($1+$2)*100/($3),$3;
dump intermediateoutput;

result = filter intermediateoutput by $1>70 and $2>1000;
final = order result by $1 desc;
dump final;

store finaloutput into 'hdfs://localhost:54310/niit/pig/question9' using PigStorage('\t');

