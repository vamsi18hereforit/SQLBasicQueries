-- Databricks notebook source
create database clinic;
USE DATABASE CLINIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating dataframe for each csv file by using inferSchema
-- MAGIC pets = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/P9_Pets.csv')
-- MAGIC owners = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/P9_Owners.csv')
-- MAGIC details = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/P9_ProceduresDetails.csv')
-- MAGIC history = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/P9_ProceduresHistory.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Creating tables using the dataframes created above
-- MAGIC pets.write.mode("overwrite").saveAsTable("clinic.pets")
-- MAGIC owners.write.mode("overwrite").saveAsTable("clinic.owners")
-- MAGIC details.write.mode("overwrite").saveAsTable("clinic.details")
-- MAGIC history.write.mode("overwrite").saveAsTable("clinic.history")

-- COMMAND ----------

SELECT * FROM pets

-- COMMAND ----------

SELECT * FROM owners

-- COMMAND ----------

SELECT * FROM details

-- COMMAND ----------

SELECT * FROM history

-- COMMAND ----------

SELECT P.Name AS PET_NAME, P.Kind, O.Name OWNER_NAME
FROM PETS P
JOIN OWNERS O ON P.OWNERID = O.OWNERID
-- WHERE P.Name = 'Lakshmi'


-- COMMAND ----------

SELECT Kind, count(*)
FROM pets 
GROUP BY Kind

-- COMMAND ----------

-- CHECKING FOR DUPLICATES
SELECT Name, OwnerID, count(*)
FROM owners 
GROUP BY Name,OwnerID
HAVING count(*)>1

-- COMMAND ----------

-- GET PETNAME FROM HISTORY WITH FIRST AND LAST VISITS
SELECT P.PETID, P.Name,min(H.DATE) AS FIRST, max(H.DATE) AS LAST
FROM pets P 
JOIN history H ON P.PETID = H.PETID
GROUP BY P.PETID, P.Name 


-- COMMAND ----------

-- SLECT ONLY NEW PETS THAT DIDNT HAVE HISTORY and 
SELECT P.PetID, P.Name, P.Kind, O.Name
FROM PETS P
JOIN OWNERS O ON P.OwnerID = O.OwnerID
WHERE P.PetID NOT IN (SELECT PETID FROM history) 
-- AND P.KIND = "Cat"
AND O.NAME LIKE "____"
SORT BY P.KIND

-- COMMAND ----------

--  GET 2ND HIGHEST PRICE IN DETAILS
SELECT * FROM details
WHERE PRICE = (SELECT max(PRICE)
FROM details 
WHERE PRICE < (SELECT MAX(Price) FROM details) )

-- COMMAND ----------

(SELECT *, DENSE_RANK() OVER (PARTITION BY PROCEDURETYPE ORDER BY PRICE DESC) AS R
FROM DETAILS )

-- COMMAND ----------

(SELECT *, NTILE(2) OVER (PARTITION BY PROCEDURETYPE ORDER BY PRICE DESC) AS R
FROM DETAILS )
