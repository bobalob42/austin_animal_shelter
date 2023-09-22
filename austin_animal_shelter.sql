-- Use row_number with partition by Animal ID to create unique id's to join both tables.
-- Starting with intakes table first.

select
	concat(abc.Animal_ID, "-", occur) as uniID_i
from (
	select
		i.Animal_ID,
		row_number() over (partition by i.Animal_ID) as occur
	from austin_animal_center_intakes i
	order by Animal_ID) abc;

-- Outcomes table second.

select 
	concat(xyz.`Animal ID`, "-", occur) as uniID_o
from (
	select
		o.`Animal ID`,
        row_number() over (partition by o.`Animal ID`) as occur
	from austin_animal_center_outcomes o
    order by `Animal ID`) xyz;
    
-- Generate the new ID's and insert into new column with Excel to re-import.

insert into austin_animal_center_intakes (new_ID)
select
	concat(abc.Animal_ID, "-", occur)
from (
	select
		i.Animal_ID,
        `DateTime`,
		row_number() over (partition by i.Animal_ID) as occur
	from austin_animal_center_intakes i
	order by 1, 2) abc;

-- Repeat with outcome table, creating new column, and id's to match arrivals with multiple visits.

alter table austin_animal_center_outcomes
ADD new_ID2 text;

-- Add id's in.

insert into austin_animal_center_outcomes (new_ID2)
select
	concat(abc.`Animal ID`, "-", occur)
from (
	select
		o.`Animal ID`,
        `DateTime`,
		row_number() over (partition by o.`Animal ID`) as occur
	from austin_animal_center_outcomes o
	order by 1,2) abc;
    
-- View updated table with id's. Export and most new values to appropriate column.

select *
from austin_animal_center_outcomes;

-- Group arrivals and outcomes by month and year by cat and dog.

with cte as (
	select
		i.new_ID,
        i.`DateTime`,
        o.`DateTime` as dtime,
        i.`Animal Type`,
		cast(concat(substring(i.DateTime,7,4),'/',substring(i.DateTime,1,5)) as date) as intake_date,
		cast(concat(substring(o.DateTime,7,4),'/',substring(o.DateTime,1,5)) as date) as outcome_date,
        o.`Outcome Type`
	from atx_animals_id2_intake i
    left join atx_animals_id2_outcome o on i.new_ID = o.new_ID2
)

select
	extract(year_month from cte.intake_date) as `Year Month`,
    cte.`Animal Type`,
    count(cte.`DateTime`) as Intakes,
    count(cte.dtime) as Outcomes
from cte
group by `Year Month`, cte.`Animal Type`
order by 1;	

-- View all outcome types.

select 
	`Outcome Type`,
    count(*) as Outcomes
from atx_animals_id2_outcome
group by `Outcome Type`
order by Outcomes desc;

-- View all arrival types by Animal Type

select
	`Animal Type`,
    count(*) as Intakes
from atx_animals_id2_intake
group by `Animal Type`
order by Intakes desc;

-- View all arrival types by Arrival Type.

select
	`Intake Type`,
    count(*) as Intakes
from atx_animals_id2_intake
group by `Intake Type`
order by Intakes desc;