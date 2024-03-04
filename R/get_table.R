#' get table
#'
#' get the patient data
#'
#' @export
get_table <- function(dbf,tabname=NULL,fields='*',whereclause=NULL){
  if(F){
    dbf <- dbfile
    tabname <- "drug_exposures"
    fields <- '*'
    whereclause <- NULL
  }

  strsql <- str_c("
    SELECT DISTINCT
    ",paste0(fields,collapse=", "),"
    FROM 
    ",tabname,";")
  if(!is.null(whereclause)){
    strsql <- cat(strsql,"\n",whereclause)
  }
  dbi <- dbConnect(duckdb(),dbf)
  pats <- dbGetQuery(dbi,strsql) %>% tibble()
  dbDisconnect(dbi)
  return(pats)
}
      
