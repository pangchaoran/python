
*libname base "F:\python\sas\data";
libname base "C:\Users\PangC\Pictures\macros\python\data";

%let rawdata_path=C:\Users\PangC\Pictures\macros\python\rawdata\;
%macro import(code=);
  dm 'log;clear;';
  %if %sysfunc(fileexist(&rawdata_path.&code..csv)) %then %do;
    proc import datafile="&rawdata_path.&code..csv"
    out=base.b&code.
    dbms=csv replace;
    getnames=yes;
    proc sort; by date; run;
  %end;
%mend;

%macro all1;
  proc import datafile="&rawdata_path.stock_basics.csv"
  out=b1
  dbms=csv replace;
  getnames=yes;
  run;

  data b2(keep=code1);
  set b1;
  where code>=600200;
  code1=put(code,z6.);
  proc sort; by code1; run;

  %let dsid=%sysfunc(open(b2,i));
  %let code1_n=%sysfunc(varnum(&dsid.,code1));
  %let i=0;
  %do %while(%sysfunc(fetch(&dsid)) eq 0 );
    %let code1=%sysfunc(getvarc(&dsid,&code1_n.));
    %import(code=&code1.);
    
    %let i=%eval(&i+1);
  %end;
  %let rc=%sysfunc(close(&dsid.));
%mend;

option nomprint;
%all1;










