USE world_layoffs;

-- Data Cleaning

SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- DELETE DUPLICATES

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date') as Row_Nmbr
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as Row_Nmbr
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE Row_Nmbr > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Ola';


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
  `Row_Nmber` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as Row_Nmbr
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE Row_Nmber > 1;

DELETE
FROM layoffs_staging2
WHERE Row_Nmber > 1;

-- STANDARDIZE DATA

SELECT company
FROM layoffs_staging;

UPDATE layoffs_staging
SET company = TRIM(company);

SELECT DISTINCT industry 
FROM layoffs_staging
ORDER BY 1;

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging
ORDER BY 1;

SELECT *
FROM layoffs_staging
WHERE country LIKE 'United States';

SELECT *
FROM layoffs_staging
;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- DATE (STRING) TO DATE FORMAT

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging
;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL OR industry = '';

SELECT *
FROM layoffs_staging
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging
WHERE industry IS NULL;

UPDATE layoffs_staging
SET industry = null
WHERE industry = '';

UPDATE layoffs_staging
SET industry = 'Travel'
WHERE company = 'Airbnb' AND industry IS NULL;


UPDATE layoffs_staging
SET industry = 'Consumer'
WHERE company = 'Juul' AND industry IS NULL;

Select *
FROM layoffs_staging
WHERE company = 'Juul';







