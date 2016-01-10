
data base1;
  set base0;
  by code;
  
  lag_xa=(lag(ma5)-lag(ma10))/(lag(ma10)-lag(ma5)/2);  
  xa=(ma5-ma10)/(ma10-ma5/2);
  
  retain fl1 0;
  if first.code then fl1=0;
  if fl1 then open1=.;
  else open1=open;
  if .<lag_xa<-0.02 and xa>0.02 then fl1=1;
  
  keep code n open open1;
run;

proc sql; create table base2 as
  select a.*
  from base1 a
  join (select distinct code from base1 where open1=.) b on a.code=b.code
  order by code,n
  ;


