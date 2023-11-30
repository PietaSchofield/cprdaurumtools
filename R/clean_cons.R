#' remove consultations before year of birth this 
#' 
#' this wont work in duckdb
#'
#' @export
clean_cons <- function(dbf,warn=T,doit=F){
  if(F){
    dbf <- dbfile
    warn <- T
    doit <- F
  }
  strsql <- str_c("
    SELECT 
      c.*,
      p.yob,
      p.regstartdate
    FROM 
      consultations AS c
    INNER JOIN 
      patients AS p
      ON p.patid = c.patid
    WHERE 
      YEAR(CAST(consdate AS DATE)) - yob < 0")
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  test <- dbGetQuery(dbi,strsql) %>% tibble()
  duckdb::dbDisconnect(dbi)
  if(warn){
    cat(paste0(nrow(test)," records will be deleted from consultations\n"))
  }
}

