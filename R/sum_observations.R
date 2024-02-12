#' Summarise observations
#'
#'
#' @export
sum_observations <- function(dbf, indexcode=NULL,db=F){
  if(db){
    dbf <- dbfile
    cohort <- "sle"
  }
  ddbc <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  sum_sql <- str_c(" 
    SELECT 
      patid,
      medcodeid,
      first_obs,
      last_obs,
      COUNT(medcodeid)
    FROM(
      SELECT
        p.patid,
        medcodeid,
        FIRST_VALUE(obsdate) OVER (PARTITION BY o.patid, medcodeid ORDER BY obsdate) AS first_obs,
        FIRST_VALUE(obsdate) OVER (PARTITION BY o.patid, medcodeid ORDER BY obsdate DESC) AS last_obs
      FROM 
        patients AS p
      INNER JOIN
        fullindexages AS i
        ON i.patid=p.patid 
      LEFT JOIN 
        observations AS o
        ON p.patid=o.patid
      WHERE 
        codetype LIKE '",cohort,"' AND
        o.obsdate < CAST(i.indexdate AS DATE)
        ) AS tmp
    GROUP BY
      patid,
      medcodeid,
      first_obs,
      last_obs")

  sum_obs <- dbGetQuery(ddbc,sum_sql) %>% tibble()
  duckdb::dbDisconnect(ddbc)
  sum_obs
}
