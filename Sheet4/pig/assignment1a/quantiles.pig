REGISTER PigQuantiles.jar

A = LOAD 'input' USING PigStorage(';') AS (date:chararray, station:int, hour:int, temp:double);

OA = ORDER A BY temp DESC;

B = GROUP OA BY hour;

C = FOREACH B {
  numelements = COUNT(OA.temp);
  
  --Create element position from Bag
  floorq1 = (numelements == 1 ? 1 : ((int) FLOOR(0.25 * (numelements + 1))));
  ceilq1 = (numelements == 1 ? 1 : (int) CEIL(0.25 * (numelements + 1)));
  floorq3 = (numelements == 1 ? 1 : (int) FLOOR(0.75 * (numelements + 1)));
  ceilq3 = (numelements == 1 ? 1 : (int) CEIL(0.75 * (numelements + 1)));

  q1 = TOTUPLE(floorq1, ceilq1, OA.temp);
  q3 = TOTUPLE(floorq3, ceilq3, OA.temp);

  GENERATE group, assignment1.PigQuantiles(q1), assignment1.PigQuantiles(q3);
}

DUMP C;
STORE C INTO 'output';
