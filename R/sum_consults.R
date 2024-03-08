#' Summarise consultations
#'
#' Find the number of consultations prior to the indexdate of first diagnosis. 
#'
#'
#' @export
sum_consults <- function(dbf,cases='%',short=T,db=F){
  if(db){
    #short <- F
    #dbf <- dbfile
    #cases <- "ibd"
  } 
  sum_sql <- str_c(" 
    SELECT 
      patid,
      yob,
      cohort,
      codetype,
      indexdate,
      first_obs,
      last_obs,
      DATE_DIFF('MONTH',CAST(first_obs AS DATE),CAST(last_obs AS DATE)) AS period,
      COUNT(consid) AS freq
    FROM(
      SELECT DISTINCT
        p.patid,
        p.yob,
        i.codetype,
        i.cohort,
        i.indexdate,
        consid,
        FIRST_VALUE(consdate) OVER (PARTITION BY o.patid ORDER BY consdate) AS first_obs,
        FIRST_VALUE(consdate) OVER (PARTITION BY o.patid ORDER BY consdate DESC) AS last_obs
      FROM 
        patients AS p
      LEFT JOIN
        fullindexages AS i
        ON i.patid=p.patid 
      LEFT JOIN 
        consultations AS o
        ON p.patid=o.patid
      WHERE 
        codetype LIKE '",cases,"' AND
        o.consdate < CAST(i.indexdate AS DATE)
        ) AS tmp
    GROUP BY
      patid,
      yob,
      codetype,
      cohort,
      indexdate,
      first_obs,
      last_obs")

  dbc <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  res <- dbGetQuery(dbc,sum_sql) %>% tibble()
  duckdb::dbDisconnect(dbc,shutdown=T)
  if(short){
    res %>% mutate(freq=freq/(1+period)) %>% group_by(cohort,codetype) %>%
      summarise(freq=mean(freq),.groups="drop")
  }else{
    res
  }
}
