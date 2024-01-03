--Tabellen students und exams erstellen
create table students (
    id    integer not null primary key,
    name  varchar(50),
    major varchar(50),
    year  integer
);

create table exams (
    id         integer not null primary key,
    sid        integer,
    grade      numeric(2, 1),
    course     varchar(50),
    curriculum varchar(50),
    date       integer
);

INSERT INTO students (id, name, major, year) VALUES
(1, 'Alice', 'CS', 2019),
(2, 'Bob', 'CS', 2019),
(3, 'Charlie', 'CS', 2019),
(4, 'David', 'Games Eng', 2019),
(5, 'Eva', 'Games Eng', 2019),
(6, 'Frank', 'Games Eng', 2019),
(7, 'George', 'Informatik', 2019),
(8, 'Hannah', 'Informatik', 2019),
(9, 'Ivan', 'Informatik', 2019),
(10, 'Julia', 'CS', 2020),
(11, 'Kevin', 'CS', 2020),
(12, 'Laura', 'CS', 2020),
(13, 'Mike', 'Games Eng', 2020),
(14, 'Nina', 'Games Eng', 2020),
(15, 'Oscar', 'Games Eng', 2020),
(16, 'Patricia', 'Informatik', 2020),
(17, 'Quentin', 'Informatik', 2020),
(18, 'Rachel', 'Informatik', 2020),
(19, 'Steve', 'CS', 2021),
(20, 'Tina', 'CS', 2021),
(21, 'Ursula', 'CS', 2021),
(22, 'Victor', 'Games Eng', 2021),
(23, 'Wendy', 'Games Eng', 2021),
(24, 'Xavier', 'Games Eng', 2021),
(25, 'Yvonne', 'Informatik', 2021),
(26, 'Zack', 'Informatik', 2021),
(27, 'Amy', 'Informatik', 2021),
(28, 'Brian', 'CS', 2022),
(29, 'Claire', 'CS', 2022),
(30, 'Dylan', 'CS', 2022),
(31, 'Elaine', 'Games Eng', 2022),
(32, 'Frank', 'Games Eng', 2022),
(33, 'Grace', 'Games Eng', 2022),
(34, 'Henry', 'Informatik', 2022),
(35, 'Isla', 'Informatik', 2022),
(36, 'Jack', 'Informatik', 2022);

INSERT INTO exams (id, sid, grade, course, curriculum, date) VALUES
(1, 1, 2.0, 'CS-course1', 'CS', 2019),
(2, 1, 3.3, 'CS-course2', 'CS', 2021),
(3, 2, 1.7, 'CS-course1', 'CS', 2019),
(4, 2, 2.3, 'CS-course2', 'CS', 2020),
(5, 3, 2.7, 'CS-course1', 'CS', 2019),
(6, 3, 3.0, 'CS-course2', 'CS', 2021),
(7, 4, 3.7, 'Games Eng-course1', 'Games Eng', 2019),
(8, 4, 1.0, 'Games Eng-course2', 'Games Eng', 2020),
(9, 5, 1.3, 'Games Eng-course1', 'Games Eng', 2019),
(10, 5, 4.0, 'Games Eng-course2', 'Games Eng', 2022),
(11, 6, 1.0, 'Games Eng-course1', 'Games Eng', 2019),
(12, 6, 3.7, 'Games Eng-course2', 'Games Eng', 2021),
(13, 10, 2.3, 'CS-course1', 'CS', 2020),
(14, 10, 1.7, 'CS-course2', 'CS', 2022),
(15, 11, 2.0, 'CS-course1', 'CS', 2020),
(16, 11, 3.0, 'CS-course2', 'CS', 2021),
(17, 12, 3.3, 'CS-course1', 'CS', 2020),
(18, 12, 2.7, 'CS-course2', 'CS', 2022),
(19, 13, 3.7, 'Games Eng-course1', 'Games Eng', 2020),
(20, 13, 1.3, 'Games Eng-course2', 'Games Eng', 2022),
(21, 14, 2.0, 'Games Eng-course1', 'Games Eng', 2020),
(22, 14, 4.0, 'Games Eng-course2', 'Games Eng', 2021),
(23, 15, 1.7, 'Games Eng-course1', 'Games Eng', 2020),
(24, 15, 2.3, 'Games Eng-course2', 'Games Eng', 2021),
(25, 19, 2.7, 'CS-course1', 'CS', 2021),
(26, 19, 3.0, 'CS-course2', 'CS', 2023),
(27, 20, 3.3, 'CS-course1', 'CS', 2021),
(28, 20, 2.7, 'CS-course2', 'CS', 2022),
(29, 21, 1.0, 'CS-course1', 'CS', 2021),
(30, 21, 3.7, 'CS-course2', 'CS', 2023),
(31, 22, 1.3, 'Games Eng-course1', 'Games Eng', 2021),
(32, 22, 4.0, 'Games Eng-course2', 'Games Eng', 2022),
(33, 23, 2.0, 'Games Eng-course1', 'Games Eng', 2021),
(34, 23, 2.3, 'Games Eng-course2', 'Games Eng', 2023),
(35, 24, 1.7, 'Games Eng-course1', 'Games Eng', 2021),
(36, 24, 3.0, 'Games Eng-course2', 'Games Eng', 2022),
(37, 28, 3.3, 'CS-course1', 'CS', 2022),
(38, 28, 2.7, 'CS-course2', 'CS', 2023),
(39, 29, 1.0, 'CS-course1', 'CS', 2022),
(40, 29, 3.7, 'CS-course2', 'CS', 2023),
(41, 30, 1.3, 'CS-course1', 'CS', 2022),
(42, 30, 4.0, 'CS-course2', 'CS', 2023),
(43, 31, 2.0, 'Games Eng-course1', 'Games Eng', 2022),
(44, 31, 2.3, 'Games Eng-course2', 'Games Eng', 2023),
(45, 32, 1.7, 'Games Eng-course1', 'Games Eng', 2022),
(46, 32, 3.0, 'Games Eng-course2', 'Games Eng', 2023),
(47, 33, 3.3, 'Games Eng-course1', 'Games Eng', 2022),
(48, 33, 2.7, 'Games Eng-course2', 'Games Eng', 2023);

--Query 2, Ausgangsform
select s.name, e.course
from   students s, exams e
where  s.id=e.sid and
   (s.major = 'CS' or s.major = 'Games Eng') and
   e.grade>=(select avg(e2.grade)+1 --one grade worse
           from exams e2          --than the average grade
           where s.id=e2.sid or   --of exams taken by
                 (e2.curriculum=s.major and --him/her or taken
                 s.year>e2.date));         --by elder peers

--Query 2, Optimierte Form nach Neumann
WITH D AS (
   SELECT DISTINCT s.id, s.year, s.major
   FROM students s, exams e
   WHERE s.id = e.sid and (s.major = 'CS' OR s.major = 'Games Eng')
)
SELECT s.name, e.course
FROM students s, exams e, (
   SELECT AVG(e2.grade) m, D.id, D.year, D.major
   FROM D, exams e2
   WHERE D.id = e2.sid or (D.year > e2.date and e2.curriculum = D.major)
   GROUP BY D.id, D.major, D.year
) AS d
WHERE s.id = e.sid AND (s.major = 'CS' OR s.major = 'Games Eng') AND
      e.grade > m+1 AND (d.id = s.id or (d.year > e.date and e.curriculum = d.major));

--Query 2, Angepasste Form
WITH D AS (
   SELECT DISTINCT s.id, s.year, s.major
   FROM students s, exams e
   WHERE s.id = e.sid and (s.major = 'CS' OR s.major = 'Games Eng')
)
SELECT s.name, e.course
FROM students s, exams e, (
   SELECT AVG(e2.grade) m, D.id, D.year, D.major
   FROM D, exams e2
   WHERE D.id = e2.sid or (D.year > e2.date and e2.curriculum = D.major)
   GROUP BY D.id, D.major, D.year
) AS d
WHERE s.id = e.sid AND (s.major = 'CS' OR s.major = 'Games Eng') AND
      e.grade > m+1 AND (d.id = s.id and d.year = s.year and d.major = s.major);