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

data _null_;
  infile "C:\python\rawdata\&date..csv" lrecl=32767 obs=1;
  informat vars $500.;
  input vars $;
  call symput('vars',prxchange('s/((code|name|exchangeCD)(\s|$))/\1\$ /',-1,tranwrd(vars,',',' ')));
run;

data base0;
  infile "C:\python\rawdata\&date..csv" delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
  informat date yymmdd10. timeToMarket yymmdd8.;
  format date timeToMarket yymmdd10.;
  input &vars.;
proc sort; by code date; run;

proc import datafile="C:\python\TradeCal.csv" dbms=csv replace
out=TradeCal(where=(isOpen=1) keep=isOpen calendarDate exchangeCD);
getnames=yes;
run;

*dummy date;
proc sql; create table base01(drop=date rename=(calendarDate=date)) as
  select e.*,d.calendarDate
  from (
    select distinct a.code,c.calendarDate,c.exchangeCD
    from (select distinct code from base0) a, 
         (select min(date) as min_date, max(date) as max_date from base0) b, 
         TradeCal c
    where max_date>=calendarDate>=min_date
  ) d left join base0 e on d.code=e.code and d.exchangeCD=e.exchangeCD and d.calendarDate=e.date
  where e.code^=''
;
create table base02 as 
  select a.*,out1
  from base01 a
  join (select code,outstanding*close/1e4 as out1 from base01 where date=input("&date.",yymmdd10.) ) b on a.code=b.code
  order by code,date
;quit;



*--------------------combine data---------------------------------------------------;
data _null_;
  length varab var $190;
  infile datalines dlm=',';
  input varab $ var $;
  call symput('var'||varab,strip(var));
  if index(varab,'a') then call symputx('varan',compress(varab,'ab '));
  if index(varab,'b') then call symputx('varbn',compress(varab,'ab '));
  if index(varab,'b') and length(var)>1 then call symputx('varbn_s',compress(varab,'ab '));
datalines;
a1,code
a2,name
a3,date
a4,out1
a5,v_ma10
a6,open
a7,adjFactor
b1,open
b2,close
b3,volume
b4,ma5
b5,ma10
b6,ma20
b7,v_ma5
b8,v_ma10
b9,v_ma20
b10,a
b11,b
b12,c
;


%macro out(n=,m=);
data base1;
  set base02;
  by code;
  n=_n_;
  xa=(ma5-ma10)/(ma10-ma5/2);
  xb=(ma5-ma20)/(ma20-ma5/2);
  xc=(ma10-ma20)/(ma20-ma10/2);
 
  lag_xa=(lag(ma5)-lag(ma10))/(lag(ma10)-lag(ma5)/2);
  %do i=1 %to &varbn_s;
    x&&varb&i=&&varb&i/lag(&&varb&i);
  %end;
  %do i=1 %to &varbn;
	x2&&varb&i=x&&varb&i ** 2;
	x3&&varb&i=x&&varb&i ** 3;
	x4&&varb&i=x&&varb&i ** 4;
	x5&&varb&i=x&&varb&i ** 5;
  %end;
  
  if ^first.code then output;
run;

data base_n1(keep=n n_old_new i rename=(n=n_by));
  set base1(where=(xa-lag_xa>0.01 and xa>0 and lag_xa<0 and v_ma20>1e4 and out1<1e3 and 5<open<60));
  do i=-&m to &n;
    n_old_new=n+i;
	output;
  end;
run;

proc sql; create table base2 as
select b.*,a.i,a.n_by
from base_n1 a
join base1 b on a.n_old_new=b.n
where open>.
group by a.n_by
having count(distinct code)=1 and count(1)>&m
order by n_by
;quit;

%macro merge(i); 
  code=strip(code)||' base2(where=(i='||strip(put(i,best.))||')';
  *rename ...;
  i_var=strip(tranwrd(put(i,best.),'-','_'));
  code=strip(code)||' rename=(adjFactor=adjFactor'||i_var;
  %do i=1 %to &varbn;
    code=strip(code)||" x&&varb&i=x&&varb&i"||i_var;
    code=strip(code)||" x2&&varb&i=x2&&varb&i"||i_var;
    code=strip(code)||" x3&&varb&i=x3&&varb&i"||i_var;
    code=strip(code)||" x4&&varb&i=x4&&varb&i"||i_var;
    code=strip(code)||" x5&&varb&i=x5&&varb&i"||i_var;
  %end;
  code=strip(code)||')';
  *keep ...;
  code=strip(code)||" keep=n_by i adjFactor";
  %do i=1 %to &varbn;
    code=strip(code)||" x&&varb&i x2&&varb&i x3&&varb&i x4&&varb&i x5&&varb&i";
  %end;
  if i=0 then code=strip(code)||" code name date";
  code=strip(code)||')';
%mend;

data code;
  length code code1 $32767 i_var $3;
  *data base3 where= ...;
  code='data base3(where=(';
  %do i=1 %to &m;
    code=strip(code)||" ^(.<adjFactor_&i<0.9) and";
  %end;
  code=strip(code)||' ^(.<adjFactor0<0.9) )); merge';
  *merge base2 ...;
  do i=-&m to 0;
    %merge(&i)
  end;
  code1=code;
  code='';
  do i=1 to &n;
    %merge(&i)
  end;
  code=' '||strip(code)||'; by n_by; run;';
run;

data _null_;
  set code;
  call execute(code1);
  call execute(code);
run;
%mend;

%out(n=4,m=20);


*----------------------------------------------analysis-----------------------------------------------*;
data r1;
  set base3(where=(xa0-xa_1>0.06));
  y432=(xopen4*xopen3*xopen2)**(1/3)-1;
  y43=(xopen4*xopen3)**(1/2)-1;
  observation=_n_;
run;
/*
data z1;
set r1;
do i=10 to 60;
if xa0-xa_1>i*0.001 then output;
end;
keep i y432;
run;

proc sql; create table z2 as
select i,count(1),count(case when y432>0 then 1 else . end)/count(1) as rate
from z1
group by i
order by 1;
*/

proc sql; create table r2 as
select r1.*,b.w
from r1 
join (select date,1/count(1) as w from r1 group by date) b on r1.date=b.date
;quit;

%macro reg;
dm 'output;clear;';
ods graphics off;
ods listing close;
ods output outputstatistics=res FitStatistics=FitStat;
proc reg data=r2;
  model y432=
    %do j=1 %to &varbn;
      x&&varb&j..0 x2&&varb&j..0 x3&&varb&j..0 x4&&varb&j..0 
      %do i=1 %to 20; x&&varb&j.._&i x2&&varb&j.._&i x3&&varb&j.._&i x4&&varb&j.._&i %end;
    %end;
  /ALPHA=0.2 cli r;
  weight w;
  */vif selection=stepwise stb sle=0.05 sls=0.05;
run;quit;
ods output close;
ods listing;
%mend;
%reg;

proc sort data=res; by observation dependent; run;

data r3;
  length code name $50 date depvar predictedvalue lowercl 8;
  merge r2(keep=observation date code name) res(drop=model);
  by observation;
  m=substr(put(date,yymmdd10.),1,7);
  if lowercl>10/3/3000; *and date>input("&date.",yymmdd10.)-5; *and abs(y432-predictedvalue)>0.02; *;
proc sort; by descending date code dependent; run;

proc sql; create table r4 as
select m,count(1),exp(log(sum(depvar+1)/count(1)))
from r3
where depvar^=.
group by m
order by 1;quit;




data z1;
set base0;
if open<=min(ma5,ma10,ma20) and close>max(ma5,ma10,ma20) and 5<open<60 and date>input('2015-12-01',yymmdd10.);
proc sort; by date code;
run;

*boll;
















data _null_;
time=put(60*(time()-&start_time.),time5.);
put time=;
run;





