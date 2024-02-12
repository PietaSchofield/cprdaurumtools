#' get the cohorts
#'
#' @export 
get_cohort <- function(dbf,codetable="cohortcodes",codetype="%",minage=0,maxage=16,
                       indexfield="case",vfill=0,summarise=T){
  if(F){
    dbf <- dbfile
    codetable <- "cohortcodes"
    codetype <- "%"
    minage <- 0
    maxage <- 16
    indexfield <- "case"
    vfill <- 0
  }
  ssql <- str_c("
    SELECT DISTINCT
      p.patid,
      p.cohort,
      i.codetype,
      CASE WHEN YEAR(CAST(i.indexdate AS DATE)) - p.yob >=0 THEN 1 ELSE 0 END AS case,
      YEAR(CAST(i.indexdate AS DATE)) - p.yob  AS indexage,
      i.indexdate,
      i.indexcode 
    FROM
      patients AS p
    INNER JOIN(
      SELECT DISTINCT
        patid,
        b.codetype,
        FIRST(o.obsdate) OVER (PARTITION BY o.patid, b.codetype ORDER BY o.obsdate) AS indexdate,
        FIRST(o.medcodeid) OVER (PARTITION BY o.patid, b.codetype ORDER BY o.obsdate) AS indexcode 
      FROM 
        observations AS o
      INNER JOIN
        ",codetable," AS b
        ON o.medcodeid=b.medcodeid
      WHERE 
        b.codetype LIKE '",codetype,"') AS i
    ON i.patid=p.patid
  WHERE
    YEAR(CAST(i.indexdate AS DATE)) - p.yob <= ",maxage," AND
    YEAR(CAST(i.indexdate AS DATE)) - p.yob >= ",minage,";")

  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf,read_only=T)
  res <- dbGetQuery(dbi,ssql) %>% tibble() 
  if(summarise){
    res <- res %>% select(all_of(c("patid","cohort","codetype",indexfield))) %>%
    pivot_wider(names_from="codetype",values_from=all_of(indexfield),values_fill=vfill)
  }
  dbDisconnect(dbi,shutdown=T)
  return(res)
}
