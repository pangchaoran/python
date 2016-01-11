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



*----------------------------------;
data _null_;
  length varab var $190;
  infile datalines dlm=',';
  input varab $ var $;
  call symput('var'||varab,strip(var));
  if index(varab,'a') then call symputx('varan',compress(varab,'ab '));
  if index(varab,'b') then call symputx('varbn',compress(varab,'ab '));
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
;

data base1;
  set base02;
  by code;
  n=_n_;
  xa=(ma5-ma10)/(ma10-ma5/2);
  xb=(ma5-ma20)/(ma20-ma5/2);
  xc=(ma10-ma20)/(ma20-ma10/2);
  %do i=1 %to &varbn;
    x&&varb&i=&&varb&i/lag(&&varb&i);
  %end;
  if ^first.code then output;
  keep code name date volume open out1 adjFactor x:;
run;

%macro(n=,m=)
data base3(where=(.<xa_1<0 and xa0>0
                  and ( %do i=1 %to &m; ^(.<adjFactor_&i<0.9) and %end; %do i=0 %to 4; ^(.<adjFactor&i<0.9) and %end; 1  )
                  and nmiss(%do i=1 %to &m; xopen_&i, %end; xopen0)=0 ));

  set base1(firstobs=%eval(&m+1) rename=(code=code0 xopen=xopen0 xclose=xclose0 xvolume=xvolume0 xa=xa0 xb=xb0 xc=xc0 adjFactor=adjFactor0
                                 xma5=xma50 xma10=xma100 xma20=xma200 xv_ma5=xv_ma50 xv_ma10=xv_ma100 xv_ma20=xv_ma200));
  %do i=1 %to &n.;
    set base1(firstobs=%eval(&m+&i+1) drop=name date volume open out1
              rename=(code=code&i xopen=xopen&i xclose=xclose&i xvolume=xvolume&i xa=xa&i xb=xb&i xc=xc&i adjFactor=adjFactor&i
                      xma5=xma5&i xma10=xma10&i xma20=xma20&i xv_ma5=xv_ma5&i xv_ma10=xv_ma10&i xv_ma20=xv_ma20&i));
  %end;
  %do i=&m %to 1 %by -1;
    set base1(firstobs=%eval(&m+1-&i) drop=name date volume open out1
        rename=(code=code_&i xopen=xopen_&i xclose=xclose_&i xvolume=xvolume_&i xma5=xma5_&i xma10=xma10_&i xma20=xma20_&i 
                xv_ma5=xv_ma5_&i xv_ma10=xv_ma10_&i xv_ma20=xv_ma20_&i xa=xa_&i xb=xb_&i xc=xc_&i adjFactor=adjFactor_&i));
  %end;
  
  %do i=1 %to &n.;
    if code0^=code&i. then call missing(xopen&i,xclose&i,xhigh&i,xvolume&i,xa&i,xb&i,xc&i,xma5&i,xma10&i,xma20&i,xv_ma5&i,xv_ma10&i,xv_ma20&i,adjFactor&i);
  %end;
  %do i=1 %to &m.;
    if code0^=code_&i. then call missing(xopen_&i,xclose_&i,xvolume_&i,xma5_&i,xma10_&i,xma20_&i,xv_ma5_&i,xv_ma10_&i,xv_ma20_&i,xa_&i,xb_&i,xc_&i,adjFactor_&i);
  %end;
  
  drop %do i=1 %to &m.; code_&i %end; %do i=1 %to &n.; code&i. %end;;
run;

%mend;

option nomprint;
%out(n=6,m=20);


*----------------------------------------------analysis-----------------------------------------------*;
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















data _null_;
time=put(60*(time()-&start_time.),time5.);
put time=;
run;





