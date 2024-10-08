SELECT MIN(n.name) AS member_in_charnamed_movie
FROM cast_info AS ci,
     company_name AS cn,
     keyword AS k,
     movie_companies AS mc,
     movie_keyword AS mk,
     name AS n,
     title AS t
WHERE cn.country_code = '[us]'
  AND k.keyword = 'character-name-in-title'
  AND n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = mk.movie_id
  AND mk.keyword_id = k.id
  AND t.id = mc.movie_id
  AND mc.company_id = cn.id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id
  AND cn.name_pcode_nf !=
      (SELECT MAX(cn_sub.name_pcode_nf)
       FROM company_name AS cn_sub
       WHERE cn_sub.country_code != cn.country_code
         AND cn_sub.name_pcode_nf LIKE 'A%');

