/*
dm 'log;clear;';
libname base "C:\python\sas\data";
libname worka "C:\python\sas\work";
option user=worka;

%let start_time=%sysfunc(time());

filename process pipe 'powershell -Command "ls -name C:\python\rawdata\*.csv"';
data _null_;
  infile process truncover;
  input date $char200.;
  call symput('date',scan(date,1,'.'));
run;
*/

%inc "C:\python\sas\prog\anax.sas";

%macro vars(var);
  &var.0
  %do i=1 %to 20;
    &var._&i. 
  %end;
%mend;

data r1;
  set base3;
  where xa0-xa_1>0.039 and volume>5000 and out1<1e3 and 5<open<60;
  y432=(xopen4*xopen3*xopen2)**(1/3)-1;
  y43=(xopen4*xopen3)**(1/2)-1;
  observation=_n_;
run;

/*
data z0;
  set base3;
  where volume>10000;
  y432=(xopen4*xopen3*xopen2)**(1/3)-1;
  observation=_n_;
run;

data z1;
set z0;
do i=1 to 50;
if xa0-xa_1>i*0.001 then output;
end;
run;
proc sql; create table z2 as
select i,count(1) as all, count(case when y432>10/3/3000 then 1 else . end)/count(1) as p
from z1
group by i
order by i;quit;
*/

proc sql; create table r2 as
select r1.*,b.w
from r1 
join (select date,1/count(1) as w from r1 group by date) b on r1.date=b.date
;quit;

dm 'output;clear;';
ods graphics off;
ods listing close;
ods output outputstatistics=res FitStatistics=FitStat;
proc reg data=r2;
  model y432=%vars(xa) %vars(xb) %vars(xc) %vars(xclose) %vars(xvolume) 
  %vars(xma5) %vars(xma10) %vars(xma20) %vars(xv_ma5) %vars(xv_ma10) %vars(xv_ma20)
  /ALPHA=0.3 cli r;
  *model y43=xopen1 xclose1 %vars(xa) %vars(xb) %vars(xc) %vars(xclose) %vars(xvolume) 
  %vars(xma5) %vars(xma10) %vars(xma20) %vars(xv_ma5) %vars(xv_ma10) %vars(xv_ma20)
  /ALPHA=0.4 cli r;
  weight w;
  */vif selection=stepwise stb sle=0.05 sls=0.05;
run;quit;
ods output close;
ods listing;

proc sort data=res; by observation dependent; run;

data r3;
  length code0 name $50 date depvar predictedvalue lowercl 8;
  merge r2(keep=observation volume date code0 name) res(drop=model);
  by observation;
  m=substr(put(date,yymmdd10.),1,7);
  if lowercl>10/3/3000 and date>input("&date.",yymmdd10.)-5; *and abs(y432-predictedvalue)>0.02; *;
proc sort; by descending date code0 dependent; run;

/*
proc sql; create table r4 as
select m,count(1),exp(log(sum(depvar+1)/count(1)))
from r3
where depvar^=.
group by m
order by 1;quit;
*/

data z1;
set base0;
if open<=min(ma5,ma10,ma20) and 5<open<60 and close>max(ma5,ma10,ma20) and date>input('2015-12-01',yymmdd10.);
proc sort; by date code;
run;

*boll;

data z2;
set base0;
where date=input('2016-01-08',yymmdd10.);
if turnover>0 then total=volume/turnover*close/1e4;
run;
