SELECT MIN(n.name)  AS of_person,
       MIN(t.title) AS biography_movie
FROM aka_name AS an,
     cast_info AS ci,
     info_type AS it,
     link_type AS lt,
     movie_link AS ml,
     name AS n,
     person_info AS pi,
     title AS t
WHERE an.name LIKE '%a%'
  AND it.info = 'mini biography'
  AND lt.link = 'features'
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender = 'm'
    OR (n.gender = 'f'
        AND n.name LIKE 'B%'))
  AND pi.note = 'Volker Boehm'
  AND t.production_year BETWEEN 1960 AND 1980
  AND n.id = an.person_id
  AND n.id = pi.person_id
  AND ci.person_id = n.id
  AND t.id = ci.movie_id
  AND ml.linked_movie_id = t.id
  AND lt.id = ml.link_type_id
  AND it.id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id
  AND ci.person_role_id != (SELECT MAX(ci_sub.person_role_id)
                            FROM cast_info as ci_sub
                            WHERE ci_sub.person_id = pi.person_id
                              AND ci_sub.note IS NOT NULL);
