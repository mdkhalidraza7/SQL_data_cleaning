-- data cleaning project

use world_layoffs;

SELECT *
FROM layoffs;
                                               -- task --   
-- 1.Remove duplicates 
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns or rows

CREATE TABLE layoffs_staging                                 -- creating a copy table of orginal raw data table for backup
like layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;
                                                             -- below i wrote date like that becoz date is one function in my sql , so to distinguish i need to use ``this
SELECT *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging;
                                                                    -- adding a row_num for unique indexing to find duplicate 
with duplicate_cte as
(
SELECT *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging
)
select *                              --
from duplicate_cte                    -- found all the duplicate values
where row_num > 1;                    --

SELECT *
FROM layoffs_staging
where company = 'Casper';

delete                                         -- not possible as duplicate cte or row_num updation was not been done
from duplicate_cte                             -- so can't do delete as delete is update command
where row_num > 1;

CREATE TABLE `layoffs_staging_2` (                            -- so we made another table where we will update row_num first 
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging_2;

insert into layoffs_staging_2                              -- updating the data i.e row_num to make further delete the duplicates possible
SELECT *,
row_number() over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging_2
where row_num > 1;

delete                                        
from layoffs_staging_2                            
where row_num > 1;

SET sql_safe_updates=0;                    -- safe update was on so i need to disable it to delete tha table 

SELECT *                                   -- first step done, duplicates gone   
FROM layoffs_staging_2;

-- standardizing data( finding errors in data and correct it)

select company, trim(company)                               -- removing white space from beginning of the name
FROM layoffs_staging_2;
 
update layoffs_staging_2                                   -- updating the table 
set company = trim(company);

select distinct location 
from layoffs_staging_2;

select distinct industry          
from layoffs_staging_2
order by 1 ;

SELECT *                                   -- there where 2-3 crypto industry with slight different name like cryptocurrency etc
FROM layoffs_staging_2
where industry like 'crypto%';

                                 
update layoffs_staging_2                   -- so i made it in single crypto name, thus reducing industry category which was redundent earlier
set industry = 'Crypto'
where industry like 'crypto%';



SELECT *                                      -- found 3 empty value and one null value in industry            
FROM layoffs_staging_2
where industry = '' 
or industry is null;

SELECT *                                    -- checking value of industry from other present value of ame company so i can use it
FROM layoffs_staging_2
where company like 'airbnb%';
-- and industry = '' ;

update layoffs_staging_2                    -- now updating the empty value with corresponding value of same company    
set industry = 'Travel'
where company like 'Airbnb%';

SELECT *                                   -- same
FROM layoffs_staging_2
where company like 'Juul%';
-- and industry = '' ;

update layoffs_staging_2                   -- same
set industry = 'Consumer'
where company like 'Juul%';

SELECT *                                    -- same
FROM layoffs_staging_2
where company like 'Carvana%';
-- and industry = '' ;

update layoffs_staging_2                                -- same
set industry = 'Transportation'
where company like 'Carvana%';

SELECT *                                          -- didnt found any second row having same company name so no clue to resolve this
FROM layoffs_staging_2
where company like 'Bally%';
-- and industry = '' ;

update layoffs_staging_2                       -- so simply put NA to get rid of null values, looking better
set industry = 'NA'
where company like 'Bally%';

SELECT *                                  
FROM layoffs_staging_2;

select distinct country                    -- found one issue United States and United States. /both are same
from layoffs_staging_2
order by 1 ;


select distinct country, trim( trailing '.' from country)                    -- removing . from end
from layoffs_staging_2
order by 1 ;

update layoffs_staging_2                                                    -- actually updating in table
set country = trim( trailing '.' from country)
where country like 'United%';
 
select `date`                                                            -- now the issue is date is in text dat type which will stop us to do
from layoffs_staging_2;                                                  -- time series or any forecasting with date , so we need to change date format then data type
 
select `date`,
str_to_date(`date`, '%m/%d/%Y')                                      -- this is the code to execute first to put date in common date format                                          
from layoffs_staging_2;   
 
 update layoffs_staging_2                                           -- done 
 set `date` = str_to_date(`date`, '%m/%d/%Y');
 
 alter table layoffs_staging_2                                   -- date data type changed , you can check beside but we have done on copy table not on raw table
 modify column `date` date;                                      -- i.e. staging_2 in this 
 
 SELECT *                                   
FROM layoffs_staging_2;
 
 -- populating null or blamnk values 
 -- so industry had some null/blank values which i had already populated above along with standardizing industry
 
 SELECT *                                   
FROM layoffs_staging_2                                   -- so there are huge data where both columns have null value
where total_laid_off is null                           -- i am not getting any clue to populate and it seems that those rows are useless
and percentage_laid_off is null ;
 
delete                                 
FROM layoffs_staging_2                                   -- so got rid of it
where total_laid_off is null                           
and percentage_laid_off is null ;
 
 -- we are not touching stage and funds_raised one as it is good to go
 
 SELECT *                                 -- perfect , lets get rid of row_num column as now it is useless  
FROM layoffs_staging_2;

alter table layoffs_staging_2              -- done 
drop row_num ;

-- yay !! we have cleaned the data
-- raw data = layoffs
-- cleaned data = layoffs_staging_2 (cleaned one)
-- saved in local storage(my cmputer) as layoffs_cleaned
