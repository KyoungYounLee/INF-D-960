--query1, Ausgangsform
select s.name,e.course
       from   students s,exams e
       where  s.id=e.sid and
              e.grade=(select min(e2.grade)
                       from exams e2
                       where s.id=e2.sid);

--query1. Mit Materialized view
CREATE MATERIALIZED VIEW outerquery AS
SELECT  s.id AS student_id, s.name, s.major, s.year, e.id AS exam_id, e.grade, e.course, e.curriculum, e.date
FROM students s
JOIN exams e ON s.id = e.sid;

SELECT oq.name, oq.course
FROM outerquery oq
JOIN (
    select min(e2.grade) m, d.student_id
    from exams e2 join outerquery d on d.student_id = e2.sid
    group by d.student_id
) as subquery ON subquery.student_id = oq.student_id
where oq.grade = m;

drop materialized view outerquery;

--query1. Mit With-Anweisung
WITH outerquery AS (
    SELECT  s.id AS student_id, s.name, s.major, s.year, e.id AS exam_id, e.grade, e.course, e.curriculum, e.date
    FROM students s
    JOIN exams e ON s.id = e.sid
)
SELECT oq.name, oq.course
FROM outerquery oq
JOIN (
    select min(e2.grade) m, d.student_id
    from exams e2 join outerquery d on d.student_id = e2.sid
    group by d.student_id
) as subquery ON subquery.student_id = oq.student_id
where oq.grade = m;

--query1. Join-form ohne CTE
SELECT s.name, e.course
FROM students s
JOIN exams e ON s.id = e.sid
JOIN (
    SELECT min(grade) AS m, sid AS student_id
    FROM exams
    GROUP BY sid
) AS min_grades ON s.id = min_grades.student_id AND e.grade = min_grades.m;
