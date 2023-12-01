#' get patients
#'
#' @export
get_patients <- function(dbf,fields='*',whereclause=NULL){
  if(F){
    dbf <- dbfile
    fields <- '*'
    whereclause <- NULL
  }

  strsql <- str_c("
    SELECT DISTINCT
    ",paste0(fields,collapse=", "),"
    FROM 
      Patients ")
  if(!is.null(whereclause)){
    strsql <- cat(strsql,"\n",whereclause)
  }
  dbi <- dbConnect(duckdb(),dbf)
  pats <- dbGetQuery(dbi,strsql)
  dbDisconnect(dbi)
  return(pats)
}
      
