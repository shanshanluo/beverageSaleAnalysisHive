-- Create database - beverage, for internal tables
create database beverage;
show databases;
use beverage;

-- Create tables for all beverage branch data and beverage consume data
-- Two top tables are created - bev_branch, consume_count
create table if not exists bev_branch(beverage string, branch string) row format delimited fields terminated by ',';
create table if not exists consume_count(beverage string, count int) row format delimited fields terminated by ',';

load data inpath 'Shanshan_data/beverage/Bev_BranchA.txt' overwrite into table bev_branch;
load data inpath 'Shanshan_data/beverage/Bev_BranchB.txt' into table bev_branch;
load data inpath 'Shanshan_data/beverage/Bev_BranchC.txt' into table bev_branch;
load data inpath 'Shanshan_data/beverage/Bev_ConscountA.txt' overwrite into table consume_count;
load data inpath 'Shanshan_data/beverage/Bev_ConscountB.txt' into table consume_count;
load data inpath 'Shanshan_data/beverage/Bev_ConscountC.txt' into table consume_count;
/******
Managed Tables - drop table will delete table structure and all data files
Tables are created in hdfs folders - /user/hive/warehouse/bev_branch and /user/hive/warehouse/consume_count 
Data files are loaded to each corresponding folder as is.
*******/ 


create view consume_by_bev as 
	select beverage, sum(count) as total
    from consume_count
    group by beverage;

-- Question_1: What is the total number of consumers in branch1?
-- 1115974
select sum(consume_by_bev.total) from
    (select * from bev_branch where branch="Branch1") branch1
    join consume_by_bev on branch1.beverage=consume_by_bev.beverage;

-- Question_2: What is the total number of consumers in branch2?
-- 5099141
select sum(consume_by_bev.total) as total
	from (select beverage, branch from bev_branch where branch='Branch2') branch2
    join consume_by_bev on branch2.beverage=consume_by_bev.beverage;

-- Question_3: What is the most consumed beverage in branch1?
-- Special_cappuccino	108163
select * from
	(select * from bev_branch where branch="Branch1") branch1
	join consume_by_bev on consume_by_bev.beverage=branch1.beverage
	order by total desc
	limit 1;

-- Question_4: What are the beverages available in branch10, branch8 and branch1?
--
-- Beverages in branch10
-- 0 beverage in branch10
select distinct beverage from bev_branch where branch='branch10';

-- Beverage in branch8
-- 37 beverages in branch8
select distinct beverage from bev_branch where branch='Branch8';

-- Beverage in branch1
-- 20 beverages in branch1

-- Which branch has most variety of beverage
-- Branch7 sales most variety of beverage - 54,
-- Branch1 sales the least variety of beverage - 20
select * from
	(select branch, count(distinct beverage) as total_types from bev_branch
	group by branch) branch_bev_types
	order by total_types desc;

-- Which beverage has the most sales
-- Mild_cappuccino	109358 - best selling
-- SMALL_LATTE	46592 - least sales
select * from consume_by_bev order by total desc;
/**********************************************************************/

-- Create database - beverage_external, for external tables
create database beverage_external;
use beverage_external;

create external table bev_branch_ex(beverage string, branch string) row format delimited fields terminated by ','
   location '/user/training/shanshan_hive/bev_branch_ex';
load data inpath 'Shanshan_data/beverage/Bev_BranchA.txt' overwrite into table bev_branch_ex;
load data inpath 'Shanshan_data/beverage/Bev_BranchB.txt' into table bev_branch_ex;
load data inpath 'Shanshan_data/beverage/Bev_BranchC.txt' into table bev_branch_ex;