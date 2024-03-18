#' Summarise observations
#'
#'
#' @export
sum_observations <- function(dbf, cases=NULL,nfilter=1,db=F){
  if(db){
    dbf <- dbif
    cases <- "sle"
    nfilter <- 2
  }

  sum_sql <- str_c(" 
  SELECT DISTINCT
    p.patid,
    p.yob,
    s.indexdate,
    s.cohort,
    s.medcodeid,
    s.first_obs,
    s.last_obs,
    DATEDIFF('week',CAST(s.first_obs AS DATE),CAST(s.last_obs AS DATE)) AS timescale,
    s.occurance
  FROM 
    patients AS p
  LEFT JOIN(
    SELECT
      o.patid,
      i.cohort,
      i.indexdate,
      o.medcodeid,
      FIRST_VALUE(o.obsdate) OVER 
                (PARTITION BY o.patid, o.medcodeid ORDER BY o.obsdate) AS first_obs,
      FIRST_VALUE(o.obsdate) OVER 
                (PARTITION BY o.patid, o.medcodeid ORDER BY o.obsdate DESC) AS last_obs,
      COUNT(o.obsdate) OVER (PARTITION BY o.patid, o.medcodeid) AS occurance
    FROM
      observations AS o
    INNER JOIN
      fullindexages AS i
      ON i.patid=o.patid 
    INNER JOIN
      expandedsymptomlist AS e
      ON o.medcodeid=e.medcodeid
    WHERE 
      CAST(o.obsdate AS DATE) < CAST(i.indexdate AS DATE) AND
      i.codetype LIKE '",cases,"') AS s
    ON s.patid=p.patid")

  sct_sql <- str_c(" 
   SELECT
     e.medcodeid,
     e.snomedctconceptid,
     e.snomedctdescriptionid,
     e.term
   FROM 
     aurum_emismedicaldictionary AS e;")

  ddbc <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  sum_obs <- dbGetQuery(ddbc,sum_sql) %>% tibble()
  sct_dat <- dbGetQuery(ddbc,sct_sql) %>% tibble()
  duckdb::dbDisconnect(ddbc)
 
  sum_obs %>% dplyr::filter(occurance>=nfilter) %>% 
    dplyr::mutate(agef=lubridate::year(lubridate::ymd(first_obs))-yob,
                  agel=lubridate::year(lubridate::ymd(last_obs))-yob,
                  agei=lubridate::year(lubridate::ymd(indexdate))-yob) %>% 
    dplyr::select(patid,cohort,medcodeid,occurance,timescale,agef,agel,agei) %>%
      inner_join(sct_dat,by="medcodeid")
}
