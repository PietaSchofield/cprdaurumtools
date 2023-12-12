#' get index date for cases
#'
#'
#' @import magrittr
#' @export
get_indexdist <- function(dbf,idxtab="fullindexages",cohort="%",ctype="%"){
  if(F){
    dbf=dbfile
    ctype="%"
    cohort="%"
    idxtab="fullindexages"
  }
  ddb <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  ssql <- str_c("
    SELECT *
    FROM ",idxtab,"
    WHERE
     cohort LIKE '",cohort,"' AND
     codetype LIKE '",ctype,"';")
  res <- dbGetQuery(ddb,ssql) %>% tibble() 
  dbDisconnect(ddb,shutdown=T)
  res
}
