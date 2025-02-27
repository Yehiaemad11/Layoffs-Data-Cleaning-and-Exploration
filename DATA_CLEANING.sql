--         ((DATA CLEANING))


SELECT * 
FROM layoffs;

-- 1- remove duplicates
-- 2- standaridize the data
-- 3- NULL  values or the blank value
-- 4- remove any columns 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions ) AS row_num
FROM layoffs_staging
) 
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions ) AS row_num
FROM layoffs_staging
) 
DELETE 
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT * 
FROM layoffs_staging2
;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions ) AS row_num
FROM layoffs_staging;


DELETE  
FROM layoffs_staging2
WHERE row_num > 1;	

-- STANDARDIZING DATA --> finding issues in your data and then standardize it 

SELECT  company,(TRIM(company))
FROM layoffs_staging2;


UPDATE layoffs_staging2 
SET company = TRIM(company);


SELECT  *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%' ;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE  industry LIKE 'Crypto%';


SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1 ;

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United Sta%';


-- date 

SELECT `date`,
 STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- SOLVE THE PROBLEM OF (NULL  values or the blank value)


SELECT * from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT  * 
FROM layoffs_staging2
WHERE company = 'Airbnb';


SELECT T1.industry , T2.industry 
FROM layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
    AND T1.location = T2.location
WHERE (T1.industry IS NULL OR T1.industry = '') 
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2 T1
JOIN layoffs_staging2 T2
	ON T1.company = T2.company
 SET  T1.industry = T2.industry
WHERE T1.industry IS NULL  
AND T2.industry IS NOT NULL; 


-- 4- remove any columns and rows that  we need to remove or don't need anymore

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num ;





