%declare OUTPUT_COMMAND 'dump ';

REGISTER '/home/paepcke/contrib/PigIR.jar';

cd /E:/users/paepcke/dldev/EclipseWorkspaces/PigIR/PigScripts/CommandLineUtils/;
result = LOAD 'test.csv' AS (f1:int, f2:int, f3:int);


--cat choice1.txt;

dump result;

--($DEST == 'foo' ? cat choice1.txt : cat choice2.txt);



