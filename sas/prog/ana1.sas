
%macro rate(var);
lag_&var.=lag(&var.);
x&var.=&var./lag_&var.;
%mend;

data base1;
  set base0;
  by code;
  %rate(open) %rate(close) %rate(volume) %rate(ma5) %rate(ma10) %rate(ma20)
  %rate(v_ma5) %rate(v_ma10) %rate(v_ma20)
  
  xa=(ma5-ma10)/(ma10-ma5/2);
  xb=(ma5-ma20)/(ma20-ma5/2);
  xc=(ma10-ma20)/(ma20-ma10/2);
  
  if ^first.code then output;
  
  keep code date x:;
run;

%macro out(n=,m=);
data base2;
  set base1(rename=(code=code0 xopen=xopen0 xclose=xclose0 xvolume=xvolume0 xa=xa0 xb=xb0 xc=xc0
                    xma5=xma50 xma10=xma100 xma20=xma200 xv_ma5=xv_ma50 xv_ma10=xv_ma100 xv_ma20=xv_ma200));
  %do i=1 %to &n.;
    set base1(firstobs=%eval(&i+1) rename=(code=code&i xopen=xopen&i xclose=xclose&i xvolume=xvolume&i xa=xa&i xb=xb&i xc=xc&i
                                  xma5=xma5&i xma10=xma10&i xma20=xma20&i xv_ma5=xv_ma5&i xv_ma10=xv_ma10&i xv_ma20=xv_ma20&i));
  %end;
  %do i=1 %to &n.;
    if code0^=code&i. then call missing(xopen&i,xclose&i,xhigh&i,xvolume&i,xa&i,xb&i,xc&i,xma5&i,xma10&i,xma20&i,xv_ma5&i,xv_ma10&i,xv_ma20&i);
  %end;
  drop
  %do i=1 %to &n.;
    code&i.
  %end;;
run;


data base3(where=(.<xa_1<-0.02 and (xa0>0.02 or xb0>0 )  ));
  %do i=&m %to 1 %by -1;
    set base1(firstobs=%eval(&m.+1-&i) rename=(code=code_&i xopen=xopen_&i xclose=xclose_&i xvolume=xvolume_&i xma5=xma5_&i xma10=xma10_&i xma20=xma20_&i 
                                               xv_ma5=xv_ma5_&i xv_ma10=xv_ma10_&i xv_ma20=xv_ma20_&i xa=xa_&i xb=xb_&i xc=xc_&i));
  %end;
  set base2(firstobs=%eval(&m.+1));
  %do i=1 %to &m.;
    if code0^=code_&i. then call missing(xopen_&i,xclose_&i,xvolume_&i,xma5_&i,xma10_&i,xma20_&i,xv_ma5_&i,xv_ma10_&i,xv_ma20_&i,xa_&i,xb_&i,xc_&i);
  %end;
  drop
  %do i=1 %to &m.;
    code_&i.
  %end;;
run;

%mend;

option nomprint;
%out(n=6,m=20);




