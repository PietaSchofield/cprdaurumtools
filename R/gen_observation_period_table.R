#' Get the index data
#'
#'
#'
#' @export
gen_observation_period_table <- function(dbf,db=F,ow=F){
  if(db){
    dbf <- dbif
  }

  obsperiod_sql <- str_c("
  SELECT DISTINCT
    p.patid AS 'person.id',
    p.regstartdate AS 'observation.period.start.date',
    p.regenddate AS 'observation.period.end.date',
    'OTHER' AS source
  FROM 
    patients AS p

  UNION

  SELECT DISTINCT
    o.patid AS 'person.id', 
    FIRST(o.obsdate) OVER (PARTITION BY patid ORDER BY o.obsdate) AS 'observation.period.start.date',
    FIRST(o.obsdate) OVER (PARTITION BY patid ORDER BY o.obsdate DESC) AS 'observation.period.end.date',
    'ROUTINE_OBS' AS source
  FROM
    observations AS o
  INNER JOIN 
    measurement_codes AS m
    ON m.medcodeid = o.medcodeid

  UNION

  SELECT DISTINCT
    patid AS 'person.id',
    FIRST(issuedate) OVER (PARTITION BY patid ORDER BY issuedate) AS 'observation.period.start.date',
    FIRST(issuedate) OVER (PARTITION BY patid ORDER BY issuedate DESC) AS 'observation.period.end.date',
    'DRUGS' AS source
  FROM
    drug_issues

  UNION

  SELECT DISTINCT
    patid AS 'person.id',
    FIRST(admidate) OVER (PARTITION BY patid ORDER BY admidate) AS 'observation.period.start.date',
    FIRST(admidate) OVER (PARTITION BY patid ORDER BY admidate DESC) AS 'observation.period.end.date',
    'HOSP_AD' AS source
  FROM
    hes_diagnosis_hosp")
  
  dbc <- dbConnect(duckdb(),dbf)
  res <- dbGetQuery(dbc,obsperiod_sql) %>% tibble()
  dbDisconnect(dbc)
  res
}
