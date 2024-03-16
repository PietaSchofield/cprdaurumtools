#' Get the index data
#'
#'
#'
#' @export
gen_person_table <- function(dbf,db=F,ow=F){
  if(db){
    dbf <- dbif
  }
  idxdate_sql <- str_c("
  SELECT DISTINCT
    p.patid AS 'person.id',
    CONCAT(CAST(p.yob AS VARCHAR),  N'-06-15') AS 'date.of.birth',
    c.dod AS 'date.of.death',
    t.first_occur AS 'date.diag',
    2 AS 'dm.type',
    2 AS 'true.diag',
    COALESCE(h.gen_ethnicity,'Unknown') AS ethnicity,
    i.e2019_imd_5 AS ses
  FROM
    patients AS p
  INNER JOIN 
    patient_2019_imd AS i
    ON p.patid=i.patid
  LEFT JOIN
    hes_patient AS h
    ON p.patid=h.patid
  LEFT JOIN(
    SELECT
      o.patid,
      o.medcodeid,
      FIRST(o.obsdate) OVER (PARTITION BY o.patid ORDER BY o.obsdate) AS first_occur
    FROM
      observations AS o
    INNER JOIN
      diagnosis_codes AS d
      ON o.medcodeid=d.medcodeid) AS t
    ON t.patid=p.patid
  LEFT JOIN
    death_patient AS c
    ON c.patid=p.patid
  WHERE 
    t.first_occur >= CONCAT(CAST(p.yob AS VARCHAR),  N'-06-15') ")
  
  dbc <- dbConnect(duckdb(),dbf)
  res <- dbGetQuery(dbc,idxdate_sql) %>% tibble()
  dbDisconnect(dbc)
  res
}
