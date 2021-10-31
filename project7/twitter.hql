drop table T;
create table T (
    i int,
    f int
)
row format delimited fields terminated by ',' stored as textfile;
load data local inpath '${hiveconf:G}' overwrite into table T;
select t2.f3, COUNT(1)
from (select t.f as f2, COUNT(t.i) as f3
    from T as t
    group by t.f
) t2
group by t2.f3;