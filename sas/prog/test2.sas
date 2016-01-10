





data z1(keep=predictedvalue lowercl uppercl y4 date m);
merge res r2(keep=observation xopen4 xopen3 xopen2 m date);
by observation;
y4=(xopen4*xopen3*xopen2)**(1/3)-1;
if lowercl>0 and depvar=. then output;
run;

proc sql; create table z2 as
select m,exp(sum(log(y4+1))/count(1)),sum(y4+1)/count(1)
from z1
group by m
order by m
;quit;

proc sql noprint;
select count(case when y4>=lowercl then 1 else . end)/count(1) into:r1
from z1 
;quit;
%put &r1;










data _null_;
set estimate end=eof;
length formula $20000;
retain formula '';
formula=catx('+',formula,strip(variable)||'*('||strip(put(estimate,best.))||')' );
if eof then call symputx('formula',formula);
run;





proc univariate data=sum1(where=(Residual^=.))  normal;
  var Residual;
run;

data r3;
set r2(where=(y4^=.));
y41=abs(y4);
y21=abs(y2);
run;

proc univariate data=r3 normal;
  var y41;
run;


proc reg data=r2;
model y4= xa_1 xa1 xa2 close1 close2 ma51 ma101 ma52 ma102 /vif;
run;quit;
proc reg data=r2;
model y4= xa_1 xa1 xa2 close1 close2 ma51 ma101 ma52 ma102/selection=adjrsq cp;
run;quit;
proc reg data=r2;
model y2= xa_5 xa_4 xa_3 xa_2 xa_1 xa0 
close_5 close_4 close_3 close_2 close_1 close0 
/selection=stepwise stb sle=0.01 sls=0.01;
run;quit;





proc corr data=r2 spearman;
var y3 xa_1 xa0 xa1 close1 ma51 ma101 ma201;
run;

ods output outputstatistics=sum1;
proc reg data=r2;
model y2=xa_1 xa0/p;
model y3=xa_1 xa0 close1/p;
run;quit;
ods output close;


proc sql; create table z1 as
select mean(predictedvalue) as predictedvalue,mean(depvar) as depvar
from sum1
where predictedvalue>1
;
create table z2 as
select mean(predictedvalue) as predictedvalue,mean(depvar) as depvar
from sum1
;quit;








