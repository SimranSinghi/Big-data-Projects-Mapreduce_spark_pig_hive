drop table Employee;
drop table Department;

create table Employee (
  name string,
  dno int,
  address string)
row format delimited fields terminated by ',' stored as textfile;

create table Department (
  name string,
  dno int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath 'e.txt' overwrite into table Employee;

load data local inpath 'd.txt' overwrite into table Department;

select e.name, d.name
from Employee as e join Department as d on e.dno = d.dno;
