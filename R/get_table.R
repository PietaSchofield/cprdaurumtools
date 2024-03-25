#' get table
#'
#' get the patient data
#'
#' @export
get_table <- function(dbf,sqlstr=NULL,tabname=NULL,fields='*',whereclause=NULL){
  if(F){
    dbf <- dbfile
    tabname <- "drug_exposures"
    fields <- '*'
    whereclause <- NULL
  }
  if(is.null(sqlstr)){
    strsql <- str_c("
      SELECT DISTINCT
      ",paste0(fields,collapse=", "),"
      FROM 
      ",tabname,";")

    if(!is.null(whereclause)){
      strsql <- cat(strsql,"\n",whereclause)
    }
  }else{
    strsql <- sqlstr
  }
  dbi <- dbConnect(duckdb(),dbf)
  pats <- dbGetQuery(dbi,strsql) %>% tibble()
  dbDisconnect(dbi)
  return(pats)
}
      
