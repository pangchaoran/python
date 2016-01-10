
%macro base1(code=);
  %if %sysfunc(exist(base.b&code.)) %then %do;
    dm 'log;clear;';
    data base2;
    set base.b&code.;
    n=_n_;
    code="&code.";
    run;
    
    proc append base=base0 data=base2; run;
  %end;
%mend;


%macro all1;
  proc import datafile="&rawdata_path.stock_basics.csv"
  out=b1
  dbms=csv replace;
  getnames=yes;
  run;

  data b2(keep=code exe);
  set b1;
  code1=put(code,z6.);
  exe=cats('%base1(code=',put(code,z6.),');');
  proc sort; by code; run;

  data base0;
    length code $6 n 8;
    set base.b000001(obs=0);
  run;

  data _null_;
  set b2 end=eof;
  retain i 0;
  if 1 /* ranuni(int(&start_time.))>0.99*/ then do;
    i+1;
    call execute(exe);
  end;
  if eof then do;
    put "---------------------------------------------";
    put i=;
  end;
  run;
%mend;

option nomprint;
%all1;
