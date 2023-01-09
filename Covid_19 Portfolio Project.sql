-- COVID-19 data analysis script
-- This script contains a series of SQL queries for analyzing COVID-19 data.
-- The data is stored in two tables caaled Covid-19_deaths and Covid-19_vaccinations which includes but not limited to columns for date, location, total_cases,total_test, total_vaccinations  total_deaths etc.
-- The script is intended to be run against a Microsoft SQL Server management database.


-- Pull out general data for all locations
-- This query selects specified rows from the Covid_deaths table and returns the date, location, cases, and deaths for each row.
-- The results are sorted by location and date, so that the data for each location is grouped together and displayed in chronological order.

SELECT location, 
        date, 
		total_cases, 
		new_cases, 
		total_deaths,
		new_deaths, 
		population
FROM Covid_deaths
WHERE continent is not null
ORDER BY location ASC, date ASC;

-- Comparing total cases to total deaths in all countries 
SELECT location, 
       date,
       total_cases, 
	   total_deaths
FROM Covid_deaths
WHERE continent is not null
-- Calculate the percentage of total deaths to total cases for each location
-- This query selects the location, total cases, and total deaths from the Covid_deaths table,
-- and uses an expression in the SELECT clause to calculate the percentage of total deaths to total cases.
-- The resulting result set includes columns for location, total cases, total deaths, and the calculated death percentage.
SELECT location, 
       date,
       total_cases, 
       total_deaths, 
       (total_deaths / total_cases) * 100 AS death_percentage
FROM Covid_deaths

-- Drilling down to specific countries and comparing their percentage of deaths by modifynimg the above query to include the filter clause 
SELECT location,
       date,
       total_cases, 
       total_deaths, 
       (total_deaths / total_cases) * 100 AS death_percentage
FROM Covid_deaths
WHERE location IN ('Ghana', 'Kenya', 'Nigeria')
ORDER BY location ASC, date ASC

-- Compare total cases to population for each location
-- This query selects the location, date, total cases, and population from the Covid_deaths table,
-- and uses an expression in the SELECT clause to calculate the percentage of the population affected by the disease.
-- The resulting result set includes columns for location, total cases, population, and the calculated case percentage.

SELECT location,
       total_cases, 
	   population
FROM Covid_deaths

-- Compare cases to population by location , zeroing in on Ghana using the WHERE filter clause 
SELECT location,date, total_cases, population, (total_cases / population) * 100 AS case_percentage
FROM Covid_deaths
WHERE location ='Ghana'

-- Get data for Ghana and calculate total cases and average new cases per day

SELECT SUM(total_cases) AS total_cases, AVG(new_cases) AS avg_new_cases
FROM Covid_deaths
WHERE location = 'Ghana'


-- Compare effectiveness of public health measures in controlling the pandemic
-- Calculate case_death_ratio and group by location and date
SELECT location, 
       population,
       date, 
	   SUM(CAST(total_cases AS int)) AS total_cases, 
	   SUM(CAST(total_deaths AS int)) AS total_deaths,
       SUM(CAST(total_cases AS int)) / SUM(CAST(total_deaths AS int)) AS case_death_ratio
FROM Covid_deaths
WHERE continent is not null
GROUP BY location, population, date


-- Find top 10 countries with highest infection rate

SELECT TOP 10 location, 
       population, 
	   MAX(total_cases) AS max_infection_rate, 
	   MAX(total_cases / population) * 100 AS infection_percentage
FROM Covid_deaths
GROUP BY location , population 
ORDER BY infection_percentage DESC

-- Find top 20 countries with highest death  rate
SELECT TOP 20 location,  
	   MAX(CAST(total_deaths AS int)) AS death_count
FROM Covid_deaths
WHERE continent is not null
GROUP BY location 
ORDER BY death_count DESC

-- Find continent with highest death count
-- Select continent and maximum total deaths


SELECT continent,  
	   MAX(CAST(total_deaths AS int)) AS death_count
FROM Covid_deaths
WHERE continent is not null
GROUP BY continent 
ORDER BY death_count DESC

-- Find global COVID-19 statistics on deaths to cases ratio.
SELECT date,
       SUM(new_cases) AS global_cases, 
	   SUM(CAST(new_deaths as int)) AS global_deaths, 
	   SUM(CAST(new_deaths as int))/SUM(new_cases) AS death_ratio
FROM Covid_deaths
WHERE continent is not null
GROUP BY date
ORDER BY 1,2


-- Find the average reproduction rate over time for each continent
SELECT continent, AVG(CAST(reproduction_rate as float)) AS avg_reproduction_rate
FROM Covid_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY avg_reproduction_rate DESC


-- Find the locations with the highest number of weekly ICU admissions per million population
SELECT location, weekly_icu_admissions_per_million
FROM Covid_deaths
WHERE weekly_icu_admissions_per_million IS NOT NULL
ORDER BY weekly_icu_admissions_per_million DESC

-- Find the average number of weekly hospital admissions per million population for each continent
SELECT DISTINCT continent, AVG(CAST(weekly_hosp_admissions_per_million AS float)) AS avg_hosp_admissions_per_million
FROM Covid_deaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY avg_hosp_admissions_per_million DESC


-- This code utilizes the `JOIN` clause to combine data from the `Covid_deaths` and `covid_vaccinations` tables
-- based on matching 'location and date' values. The resulting data set includes all columns from both tables, allowing
--  for analysis of COVID-19 deaths and vaccination rates for each location, across different continents and dates

SELECT deaths.continent, deaths.location, deaths.population, deaths.date, vaccinations.new_vaccinations
FROM Covid_deaths deaths
JOIN covid_vaccinations vaccinations
  ON deaths.location = vaccinations.location and deaths.date = vaccinations.date
 WHERE deaths.continent is not null 
 ORDER BY 2,3


-- To calculate the percentage of the population that has received a vaccination for COVID-19.
 SELECT deaths.location, deaths.date, deaths.population, vaccinations.total_vaccinations,
       (vaccinations.total_vaccinations / deaths.population) * 100 AS vaccination_percentage
FROM Covid_deaths deaths
JOIN covid_vaccinations vaccinations
  ON deaths.location = vaccinations.location and deaths.date =vaccinations.date
WHERE deaths.location IS NOT NULL
ORDER BY 1,5

-- This code selects data from two tables and calculates the cumulative sum of vaccinations for each location over time. 
-- It filters out null values and orders the results by location and date.
SELECT deaths.continent,
deaths.location,
deaths.population,
deaths.date,
vaccinations.new_vaccinations,
SUM(CAST(vaccinations.new_vaccinations AS BIGINT)) 
OVER(partition by deaths.location ORDER BY deaths.location, deaths.date) AS Cummulative_vaccinations 
FROM Covid_deaths deaths
JOIN covid_vaccinations vaccinations
ON deaths.location = vaccinations.location AND deaths.date = vaccinations.date
WHERE deaths.continent IS NOT NULL AND vaccinations.new_vaccinations IS NOT NULL
ORDER BY deaths.location, deaths.date;



-- Combine data from two tables: 'Covid_deaths' and 'covid_vaccinations'
-- Calculate cumulative sum of 'new_vaccinations' and percentage of people vaccinated
-- Return 'continent', 'location', 'population', 'date', 'new_vaccinations', and 'total_percentage_of_people_vaccinated' columns

WITH population_to_vaccine_percentage(continent, location, population, date, new_vaccinations,  Cummulative_vaccinations)
AS (SELECT deaths.continent,
deaths.location,
deaths.population,
deaths.date,
vaccinations.new_vaccinations,
SUM(CAST(vaccinations.new_vaccinations AS BIGINT)) 
OVER(partition by deaths.location ORDER BY deaths.location, deaths.date) AS Cummulative_vaccinations 
FROM Covid_deaths deaths
JOIN covid_vaccinations vaccinations
ON deaths.location = vaccinations.location AND deaths.date = vaccinations.date
WHERE deaths.continent IS NOT NULL AND vaccinations.new_vaccinations IS NOT NULL
)
SELECT *, (Cummulative_vaccinations/population)*100 AS Total_percentage_of_people_vaccinated
FROM population_to_vaccine_percentage



