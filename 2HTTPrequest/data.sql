DROP TABLE IF EXISTS JOHNS;
DROP VIEW IF EXISTS AverageHeightWeight, AverageHeight;

/*QUESTION 0
EXAMPLE QUESTION
What is the highest salary in baseball history?
*/
SELECT MAX(salary) as Max_Salary
FROM salaries
;
/*SAMPLE ANSWER*/


/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
SELECT nameFirst, namelast, nameGiven fROM People where height > 72;


/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
CREATE Table JOHNS as
select distinct nameFirst, nameLast, People.playerID, birthState, concat(nameFirst,' ',nameLast) as nameFull from People
join CollegePlaying ON People.playerID = CollegePlaying.playerID 
where People.nameFirst = 'John' and People.birthCountry = 'USA' and CollegePlaying.schoolID = 'fordham'
;

/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
set sql_safe_updates = 0;
Delete From JOHNS
where exists(
select People.playerID from People join Batting on People.playerID = Batting.playerID
where Batting.HR < 2 and JOHNS.playerID = People.playerID);
set sql_safe_updates = 1;


/*QUESTION 4
Group together players with the same birth year, and report the year, 
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
CREATE VIEW AverageHeight(birthYear, playerCount, averageHeight)
AS
  SELECT birthYear, count(playerID), avg(height) From People 
  group by birthYear 
  order by birthYear desc;

/*QUESTION 5
Using Question 3, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
CREATE VIEW AverageHeightWeight(birthYear, playerCount, averageHeight, averageWeight)
AS
  SELECT birthYear, count(playerID), avg(height), avg(weight) from people group by ;
select * from schools where state = 'NY';

/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
SELECT People.playerID, nameFirst, namelast, schoolID from People 
Join (select HallofFame.playerID as playerID, schoolID 
	from HallofFame join(select playerID, CollegePlaying.schoolID from collegePlaying
    join Schools on CollegePlaying.schoolID = Schools.schoolID where Schools.state = 'NY') school
    on HallofFame.playerID = School.playerID) ny_halloffame
	on People.playerID = ny_halloffame.playerID order by schoolID;
-- replace with your code


/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
SELECT teamID, yearID, avg(HBP) as averageHBP
 from (select teamID, yearID, HBP from Teams where AB > 0) t
 group by teamID, yearID order by averageHBP desc
 limit 100
-- replace with your code
;
