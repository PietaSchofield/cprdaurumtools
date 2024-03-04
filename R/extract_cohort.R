#' Extract cohorts with date of exposure
#'
#'
#'
#' @export
extract_cohorts <- function(dbf,dbo,etab="cohorts",db=F,ow=F){
  if(db){
    dbf <- dbif
    dbo <- dbof
    ow <- F
  }

  cohort_sql <- str_c("
    SELECT DISTINCT
      t.patid,
      t.class,
      t.drug AS substance,
      COALESCE(e.substance,'Control') AS cohort,
      COALESCE(FIRST(e.issuedate) OVER 
               (PARTITION BY e.patid, e.substance ORDER BY e.issuedate),
              t.idate) AS substanceexposuredate,
      COALESCE(FIRST(e.issuedate) OVER 
               (PARTITION BY e.patid, t.class ORDER BY e.issuedate),
              FIRST(t.idate) OVER 
               (PARTITION BY t.patid, t.class ORDER BY t.idate)) AS classexposuredate
    FROM(
      SELECT DISTINCT
        p.patid,
        d.class,
        d.drug,
        d.idate
      FROM 
        patients AS p
      CROSS JOIN 
        drug_dates AS d) AS t
    LEFT JOIN
      drug_exposures AS e
      ON t.patid = e.patid AND t.drug = e.substance")

    
  dboc <- duckdb::dbConnect(duckdb::duckdb(),dbo)
  if(!etab%in%dbListTables(dboc) | ow){
    dbic <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    ret <- dbGetQuery(dbic,cohort_sql) %>% tibble()
    duckdb::dbDisconnect(dbic)
    dbWriteTable(dboc,etab,ret,over=T)
  }else{
    ret <- dbGetQuery(dboc,paste("SELECT * FROM ",etab))
  }
  duckdb::dbDisconnect(dboc)
  ret
}

