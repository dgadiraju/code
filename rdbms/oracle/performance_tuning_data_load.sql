drop table stock_eod;
create table stock_eod (
stock_ticker varchar2(30),
trade_date date,
open_price number,
high_price number,
low_price number,
close_price number,
volume number,
primary key (stock_ticker, trade_date));
truncate table stock_eod;

alter table stock_eod initrans 6;
alter index SYS_C007210 initrans 6;

select * from dba_tables where table_name = 'STOCK_EOD' and owner = 'PTUNING';
select * from dba_constraints where table_name = 'STOCK_EOD' and owner = 'PTUNING';
select * from dba_indexes where table_name = 'STOCK_EOD' and owner = 'PTUNING';

select count(1) from stock_eod;
select count(1) from deck_of_cards;