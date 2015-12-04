--Performance Tuning
--Only DBAs can execute below commands to create users
create user ptuning identified by ptuning;
grant create session to ptuning;
grant dba to ptuning;

--Creating tables
create table deck_of_cards (color varchar2(30), suit varchar2(30), pip varchar2(2));
truncate table deck_of_cards;
select count(1) from deck_of_cards;
select * from deck_of_cards;
select * from deck_of_cards_baseline;
create table deck_of_cards_baseline as select distinct * from deck_of_cards;
truncate table deck_of_cards_baseline;
insert into deck_of_cards_baseline select * from deck_of_cards;
explain plan for select count(1) from employees e join departments d
on e.department_id = d.department_id
;

truncate table deck_of_cards;
select * from dba_tables where table_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';

begin
for i in 1..10000
loop
  insert into deck_of_cards select * from deck_of_cards_baseline;
  commit;
end loop;
end;
/
-- 453 seconds
select count(1) from deck_of_cards;
select * from dba_tables where table_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select sum(bytes)/1024/1024 from dba_extents where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';

truncate table deck_of_cards_baseline;
insert into deck_of_cards_baseline select * from deck_of_cards;

begin
for i in 1..100
loop
  insert into deck_of_cards select * from deck_of_cards_baseline;
  commit;
end loop;
end;
/

truncate table deck_of_cards_baseline;
insert into deck_of_cards_baseline select distinct * from deck_of_cards;
commit;

truncate table deck_of_cards;

begin
for i in 1..10000
loop
  insert /*+ append */ into deck_of_cards select * from deck_of_cards_baseline;
  commit;
end loop;
end;
/

select count(1) from deck_of_cards;
select * from dba_tables where table_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select sum(bytes)/1024/1024 from dba_extents where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';

create table deck_of_cards_1g as select * from deck_of_cards where 1=2;
insert into deck_of_cards_1g select * from deck_of_cards;
commit;
select * from dba_tables where table_name = 'DECK_OF_CARDS_1G' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS_1G' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_1G' and owner = 'PTUNING';

truncate table deck_of_cards;
select count(1) from deck_of_cards_1g;

begin
for i in 1..5
loop
  insert into deck_of_cards select * from deck_of_cards_1g;
  commit;
end loop;
end;
/
--14G

select tablespace_name,status,count(*)
from dba_undo_extents
group by tablespace_name,status;

truncate table deck_of_cards;
begin
for i in 1..5
loop
  insert /*+ append */ into deck_of_cards select * from deck_of_cards_1g;
  commit;
end loop;
end;
/

drop table deck_of_cards_5g;
create table deck_of_cards_5g as select * from deck_of_cards where 1=2;
alter session enable parallel dml;
insert /*+ parallel(d 2) */ into deck_of_cards_5g d 
select /*+ parallel(d1 2) */ * from deck_of_cards d1;
commit;
select * from dba_tables where table_name = 'DECK_OF_CARDS_5G' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS_5G' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_5G' and owner = 'PTUNING';

SELECT PLAN_TABLE_OUTPUT FROM TABLE(DBMS_XPLAN.DISPLAY());

select d.department_name, j.job_title, count(1) from employees e join departments d
on e.department_id = d.department_id
join jobs j on e.job_id = j.job_id
where e.salary >= 5000
group by d.department_name, j.job_title
order by d.department_name, j.job_title;

select * from dba_tables where table_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_tablespaces where tablespace_name = 'USERS';
select * from dba_data_files where tablespace_name = 'USERS';
select * from dba_segments where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select sum(bytes)/1024/1024/1024 from dba_segments where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_1G' and owner = 'PTUNING';
select sum(blocks) from dba_extents where segment_name = 'DECK_OF_CARDS_1G' and owner = 'PTUNING';
alter session set nls_date_format='YYYY-MM-DD HH24:MI::SS';
select bytes/1024/1024, gv$log.* from gv$log;

select * from v$log_history order by first_time desc;
ALTER SYSTEM SWITCH LOGFILE;

create index DECK_OF_CARDS_IDX on deck_of_cards(pip);
select * from dba_indexes where table_name = 'DECK_OF_CARDS' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS_IDX' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_IDX' and owner = 'PTUNING';

drop table deck_of_cards_part;
CREATE TABLE "PTUNING"."DECK_OF_CARDS_PART" 
(	"COLOR" VARCHAR2(30 BYTE), 
"SUIT" VARCHAR2(30 BYTE), 
"PIP" VARCHAR2(2 BYTE)
) 
PARTITION BY LIST ("SUIT") 
(PARTITION "P1"  VALUES ('SPADE'), 
PARTITION "P2"  VALUES ('DIAMOND'), 
PARTITION "P3"  VALUES ('HEART'), 
PARTITION "P4"  VALUES ('CLUB')) ;

select * from dba_tables where table_name = 'DECK_OF_CARDS_PART' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS_PART' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_PART' and owner = 'PTUNING';

select count(1) from deck_of_cards_part;
select count(1) from deck_of_cards;

insert into deck_of_cards_part select * from deck_of_cards t;
--572 seconds
commit;
truncate table deck_of_cards_part;
alter system flush buffer_cache;

alter session enable parallel dml;
insert /*+ paralle(t2 4) */into deck_of_cards_part t2 
select /*+ parallel(t 4) */ * from deck_of_cards t;
--679 seconds
commit;

truncate table deck_of_cards_part;
alter system flush buffer_cache;
alter session enable parallel dml;
alter session enable parallel query;

alter table deck_of_cards_part initrans 1;
insert /*+ parallel(t2 4) */into deck_of_cards_part t2 
select /*+ parallel(t 4) */ * from deck_of_cards t;
--679 seconds

drop table deck_of_cards_another;
create table deck_of_cards_another as select * from deck_of_cards where 1=2;
insert into deck_of_cards_another select * from deck_of_cards_baseline;
commit;
insert into deck_of_cards_another select * from deck_of_cards_another;
select count(1) from deck_of_cards_another;
commit;

select * from dba_tables where table_name = 'DECK_OF_CARDS_ANOTHER' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS_ANOTHER' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_ANOTHER' and owner = 'PTUNING';

create table deck_of_cards_onemore as select * from deck_of_cards where 1=2;
insert into deck_of_cards_another select * from deck_of_cards_baseline;
commit;
insert into deck_of_cards_onemore select * from deck_of_cards_another;
select count(1) from deck_of_cards_another;
commit;

select * from dba_tables where table_name = 'DECK_OF_CARDS_ANOTHER' and owner = 'PTUNING';
select * from dba_segments where segment_name = 'DECK_OF_CARDS_ANOTHER' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_ANOTHER' and owner = 'PTUNING';
select * from dba_extents where segment_name = 'DECK_OF_CARDS_ONEMORE' and owner = 'PTUNING';
select * from dba_data_files where tablespace_name = 'USERS';
