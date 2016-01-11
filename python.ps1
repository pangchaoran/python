
cd C:\python
#powershell.exe -noexit C:\python\python.ps1
python get_all_hist.py

cd C:\python\sas\prog
&'C:\Program Files\SASHome\SASFoundation\9.3\sas.exe' -ICON -sysin -CONFIG 'C:\Program Files\SASHome\SASFoundation\9.3\nls\1d\sasv9.cfg' anax.sas

cd C:\python\report
git add red_cross.txt
git commit -m 'daily update'
git push origin master




