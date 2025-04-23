-- DATA CLEANING PROCESS FOR AFRICAN CONFLICTS DATASET

select *
from AfricanConflicts..african_conflicts


---------- 1. CREATING A COPY OF THE DATASET
select * 
into African_Conflicts_Edit
from AfricanConflicts..african_conflicts

select * from African_Conflicts_Edit


---------- 2. REMOVING DUPLICATES
with RowNumCTE as(
select *, row_number() 
	over (partition by actor1, actor2, admin1, admin2, admin3, country, event_id_cnty 
	order by event_id_cnty) row_num
from African_Conflicts_Edit)
select * 
from RowNumCTE
where row_num > 1

with RowNumCTE as(
select *, row_number() 
	over (partition by actor1, actor2, admin1, admin2, admin3, country, event_id_cnty 
	order by event_id_cnty) row_num
from African_Conflicts_Edit)
delete
from RowNumCTE
where row_num > 1


---------- 3. STANDARDIZING DATE FORMATS
select event_date, convert(date, EVENT_DATE, 103) as coverted
from African_Conflicts_Edit

Update African_Conflicts_Edit
set event_date = convert(date, EVENT_DATE, 103)


---------- 4. SIMPLIFYING AMBIGUOS DATA
-- IN ACTOR1 COLUMN 
select Actor_1Final from 
(select actor1,
	case
		when actor1 like '%(%' then substring(actor1, 1, charindex('(', actor1) -1)
		when actor1 not like '%(%' then actor1
	end as Actor_1Final
from African_Conflicts_Edit)
as derived_table

Update African_Conflicts_Edit
set actor1 = case
		when actor1 like '%(%' then substring(actor1, 1, charindex('(', actor1) -1)
		when actor1 not like '%(%' then actor1
			end 

-- IN ACTOR2 COLUMN
select Actor_2Final from 
(select actor2,
	case
		when actor2 like '%(%' then substring(actor2, 1, charindex('(', actor2) -1)
		when actor2 not like '%(%' then actor2
	end as Actor_2Final
from African_Conflicts_Edit)
as derived_table

Update African_Conflicts_Edit
set actor2 = case
		when actor2 like '%(%' then substring(actor2, 1, charindex('(', actor2) -1)
		when actor1 not like '%(%' then actor2
			end 

-- IN ALLY_ACTOR_1 COLUMN
select Ally_Actor_1Final from 
(select ally_actor_1,
	case
		when ally_actor_1 like '%(%' then substring(ally_actor_1, 1, charindex('(', ally_actor_1) -1)
		when ally_actor_1 not like '%(%' then ally_actor_1
	end as Ally_Actor_1Final
from African_Conflicts_Edit)
as derived_table

Update African_Conflicts_Edit
set ally_actor_1 = case
		when ally_actor_1 like '%(%' then substring(ally_actor_1, 1, charindex('(', ally_actor_1) -1)
		when ally_actor_1 not like '%(%' then ally_actor_1
			end 

-- IN ALLY_ACTOR_2 COLUMN
select Ally_Actor_2Final from 
(select ally_actor_2,
	case
		when ally_actor_2 like '%(%' then substring(ally_actor_2, 1, charindex('(', ally_actor_2) -1)
		when ally_actor_2 not like '%(%' then ally_actor_2
	end as Ally_Actor_2Final
from African_Conflicts_Edit)
as derived_table

Update African_Conflicts_Edit
set ally_actor_2 = case
		when ally_actor_2 like '%(%' then substring(ally_actor_2, 1, charindex('(', ally_actor_2) -1)
		when ally_actor_2 not like '%(%' then ally_actor_2
			end
